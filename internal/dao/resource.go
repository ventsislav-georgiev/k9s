// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of K9s

package dao

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/derailed/k9s/internal"
	"github.com/derailed/k9s/internal/client"
	"github.com/derailed/k9s/internal/slogs"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
)

var (
	_ Accessor  = (*Resource)(nil)
	_ Describer = (*Resource)(nil)
	_ Nuker     = (*Resource)(nil)
)

// Resource represents an informer based resource.
type Resource struct {
	Generic
}

// List returns a collection of resources.
func (r *Resource) List(ctx context.Context, ns string) ([]runtime.Object, error) {
	lsel := labels.Everything()
	if sel, ok := ctx.Value(internal.KeyLabels).(labels.Selector); ok && sel != nil {
		lsel = sel
	}

	f := r.getFactory()
	oo, err := f.List(r.gvr, ns, false, lsel)
	if err == nil && len(oo) > 0 {
		return oo, nil
	}
	// Cold informer: on large/loaded clusters the shared informer's initial sync
	// can take ~10-20s (paginated, consistent list over a slow konnectivity
	// link -- measured ~17s for 220 deployments vs ~3s for a one-shot list).
	// fac.List(wait=false) returns empty until then, so the view shows nothing.
	// Serve a fast direct list (RV=0, watch cache) -- cached + background
	// refreshed, never blocking -- until the informer reports synced, at which
	// point we switch back to it for live updates.
	if inf, herr := f.CanForResource(listNS(ns), r.gvr, client.ListAccess); herr == nil && inf != nil && inf.Informer().HasSynced() {
		return oo, err
	}

	return r.cachedDirectList(ns, lsel), nil
}

// directListCache holds the most recent direct-list result per
// (gvr,namespace,selector) key, with a single-flight flag so refresh ticks
// don't pile up concurrent fetches. Used only while an informer is cold.
var directListCache = struct {
	sync.Mutex
	data     map[string][]runtime.Object
	inFlight map[string]bool
}{
	data:     map[string][]runtime.Object{},
	inFlight: map[string]bool{},
}

// cachedDirectList returns the cached direct-list result for a
// (gvr,namespace,selector) and triggers a background server-side LIST (RV=0) to
// refresh it. It never blocks on the network, so it is safe on any caller.
func (r *Resource) cachedDirectList(ns string, lsel labels.Selector) []runtime.Object {
	return cachedDirectListFor(r.Client(), r.gvr, ns, lsel)
}

// cachedDirectListFor returns the cached direct-list result for an explicit
// (gvr,namespace,selector) and triggers a background server-side LIST (RV=0) to
// refresh it. It never blocks on the network, so it is safe to call from the
// event loop -- e.g. counting pods per node without syncing the cluster-wide
// pod informer (which never finishes on large clusters).
func cachedDirectListFor(c client.Connection, gvr *client.GVR, ns string, lsel labels.Selector) []runtime.Object {
	key := gvr.String() + "\x00" + ns + "\x00" + lsel.String()

	directListCache.Lock()
	cached := directListCache.data[key]
	if !directListCache.inFlight[key] {
		directListCache.inFlight[key] = true
		go func() {
			oo, err := directList(c, gvr, ns, lsel)
			directListCache.Lock()
			directListCache.inFlight[key] = false
			if err == nil {
				directListCache.data[key] = oo
			}
			directListCache.Unlock()
			if err != nil {
				slog.Error("Direct list failed", slogs.GVR, gvr, slogs.Namespace, ns, slogs.Error, err)
			}
		}()
	}
	directListCache.Unlock()

	return cached
}

// ResetDirectCaches clears every non-blocking direct-access cache (the cold
// informer list/get fallbacks: directListCache, nodePodsCache, scopedPodsCache).
// Call it on a context switch so a freshly selected cluster never paints stale
// rows carried over from the previous one (caches key on gvr/ns/selector, not
// context). With data cleared, loading indicators show until the new cluster's
// direct lists land, and a stale in-flight goroutine is overwritten on the next
// refresh tick.
func ResetDirectCaches() {
	directListCache.Lock()
	directListCache.data = map[string][]runtime.Object{}
	directListCache.inFlight = map[string]bool{}
	directListCache.Unlock()

	nodePodsCache.Lock()
	nodePodsCache.data = map[string][]runtime.Object{}
	nodePodsCache.inFlight = map[string]bool{}
	nodePodsCache.Unlock()

	scopedPodsCache.Lock()
	scopedPodsCache.data = map[string][]runtime.Object{}
	scopedPodsCache.inFlight = map[string]bool{}
	scopedPodsCache.Unlock()
}

// listNS normalizes a namespace for informer sync checks (cluster-wide/all map
// to the blank namespace the factory keys informers by).
func listNS(ns string) string {
	if client.IsClusterWide(ns) {
		return client.BlankNamespace
	}
	return ns
}

// directList performs a server-side LIST straight from the API server,
// bypassing the informer. RV=0 serves it from the apiserver watch cache.
func directList(c client.Connection, gvr *client.GVR, ns string, lsel labels.Selector) ([]runtime.Object, error) {
	dial, err := c.DynDial()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.Config().CallTimeout())
	defer cancel()

	opts := metav1.ListOptions{ResourceVersion: "0", LabelSelector: lsel.String()}
	res := dial.Resource(gvr.GVR())

	lister := res.List
	if !client.IsClusterWide(ns) {
		lister = res.Namespace(ns).List
	}
	ll, err := lister(ctx, opts)
	if err != nil {
		return nil, err
	}

	oo := make([]runtime.Object, len(ll.Items))
	for i := range ll.Items {
		oo[i] = &ll.Items[i]
	}

	return oo, nil
}

// Get returns a resource instance if found, else an error.
func (r *Resource) Get(_ context.Context, path string) (runtime.Object, error) {
	return cachedOrDirectGet(r.getFactory(), r.Client(), r.gvr, path)
}

// cachedOrDirectGet reads a single object non-blocking from the informer cache,
// falling back to a direct server-side GET (ResourceVersion=0, served from the
// apiserver watch cache) when the cache is cold. This avoids fac.Get(wait=true),
// which on a cold cache -- e.g. node-scoped drill-ins that never start the
// informer -- blocks the UI up to ~10*defaultWaitTime (~5s) and may error, and
// also avoids a consistent etcd quorum read (measured ~5s on large/loaded
// clusters vs ~0.3s from the watch cache). Read-only display paths (YAML view,
// single-instance table refresh) only; edit shells out to `kubectl edit`.
func cachedOrDirectGet(f Factory, c client.Connection, gvr *client.GVR, path string) (runtime.Object, error) {
	if o, err := f.Get(gvr, path, false, labels.Everything()); err == nil && o != nil {
		return o, nil
	}

	ns, n := client.Namespaced(path)
	clusterScoped := client.IsClusterScoped(ns)
	dial, err := c.DynDial()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.Config().CallTimeout())
	defer cancel()

	res := dial.Resource(gvr.GVR())
	opts := metav1.GetOptions{ResourceVersion: "0"}
	if clusterScoped {
		return res.Get(ctx, n, opts)
	}

	return res.Namespace(ns).Get(ctx, n, opts)
}

// FetchObject returns a single object without blocking the UI on a cold
// informer cache: it reads the cache non-blocking, then falls back to a direct
// server-side GET (RV=0). Use this from event-loop handlers (drill-ins,
// display/decode actions) instead of Factory.Get(wait=true), which on a cold
// cache blocks up to ~5s and then errors "not found" -- the sporadic freeze
// seen right after a list is painted from the direct-list fallback (informer
// not yet synced).
func FetchObject(f Factory, gvr *client.GVR, fqn string) (runtime.Object, error) {
	return cachedOrDirectGet(f, f.Client(), gvr, fqn)
}

// ToYAML returns a resource yaml.
func (r *Resource) ToYAML(path string, showManaged bool) (string, error) {
	o, err := r.Get(context.Background(), path)
	if err != nil {
		return "", err
	}

	raw, err := ToYAML(o, showManaged)
	if err != nil {
		return "", fmt.Errorf("unable to marshal resource %w", err)
	}
	return raw, nil
}
