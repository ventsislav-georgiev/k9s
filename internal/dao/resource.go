// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of K9s

package dao

import (
	"context"
	"fmt"

	"github.com/derailed/k9s/internal"
	"github.com/derailed/k9s/internal/client"
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
	if sel, ok := ctx.Value(internal.KeyLabels).(labels.Selector); ok {
		lsel = sel
	}

	return r.getFactory().List(r.gvr, ns, false, lsel)
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
