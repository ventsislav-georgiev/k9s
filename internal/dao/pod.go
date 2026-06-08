// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of K9s

package dao

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/derailed/k9s/internal"
	"github.com/derailed/k9s/internal/client"
	"github.com/derailed/k9s/internal/render"
	"github.com/derailed/k9s/internal/slogs"
	"github.com/derailed/k9s/internal/watch"
	"github.com/derailed/tview"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	restclient "k8s.io/client-go/rest"
	mv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
)

var (
	_ Accessor        = (*Pod)(nil)
	_ Nuker           = (*Pod)(nil)
	_ Loggable        = (*Pod)(nil)
	_ Controller      = (*Pod)(nil)
	_ ContainsPodSpec = (*Pod)(nil)
	_ ImageLister     = (*Pod)(nil)
)

type streamResult int

const (
	logRetryCount                  = 20
	logBackoffInitial              = 500 * time.Millisecond
	logBackoffMax                  = 30 * time.Second
	logChannelBuffer               = 50   // Buffer size for log channel to reduce drops
	streamEOF         streamResult = iota // legit container log close (no retry)
	streamError                           // retryable error (network, auth, etc.)
	streamCanceled                        // context canceled
)

// Pod represents a pod resource.
type Pod struct {
	Resource
}

// shouldStopRetrying checks if we should stop retrying log streaming based on pod status.
func (p *Pod) shouldStopRetrying(path string) bool {
	pod, err := p.GetInstance(path)
	if err != nil {
		return true
	}

	if pod.DeletionTimestamp != nil {
		return true
	}

	switch pod.Status.Phase {
	case v1.PodSucceeded, v1.PodFailed:
		return true
	default:
		return false
	}
}

// Get returns a resource instance if found, else an error.
func (p *Pod) Get(ctx context.Context, path string) (runtime.Object, error) {
	o, err := p.Resource.Get(ctx, path)
	if err != nil {
		return o, err
	}

	u, ok := o.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("expecting *unstructured.Unstructured but got `%T", o)
	}

	var pmx *mv1beta1.PodMetrics
	if withMx, ok := ctx.Value(internal.KeyWithMetrics).(bool); ok && withMx {
		pmx, _ = client.DialMetrics(p.Client()).FetchPodMetrics(ctx, path)
	}

	return &render.PodWithMetrics{Raw: u, MX: pmx}, nil
}

// ListImages lists container images.
func (p *Pod) ListImages(_ context.Context, path string) ([]string, error) {
	pod, err := p.GetInstance(path)
	if err != nil {
		return nil, err
	}

	return render.ExtractImages(&pod.Spec), nil
}

// List returns a collection of nodes.
func (p *Pod) List(ctx context.Context, ns string) ([]runtime.Object, error) {
	sel, _ := ctx.Value(internal.KeyFields).(string)
	fsel, err := labels.ConvertSelectorToLabelsMap(sel)
	if err != nil {
		return nil, err
	}
	nodeName := fsel["spec.nodeName"]

	lsel := labels.Everything()
	if s, ok := ctx.Value(internal.KeyLabels).(labels.Selector); ok && s != nil {
		lsel = s
	}

	var (
		oo      []runtime.Object
		withMx  = false
		blockMx = true
	)
	if v, ok := ctx.Value(internal.KeyWithMetrics).(bool); ok {
		withMx = v
	}
	switch {
	case nodeName != "":
		// node->pods drill-in: list this node's pods directly from the API
		// server with a server-side fieldSelector. The shared cluster-wide pod
		// informer is unusable here on large clusters (it lists & decodes EVERY
		// pod, taking minutes to sync).
		//
		// The model's first Watch refresh runs synchronously on the tcell event
		// loop; a blocking REST call there freezes the UI. So this returns
		// cached pods immediately (nil on a cold cache) and refreshes in the
		// background. The model's updater tick then picks up the fresh list.
		// Metrics are likewise non-blocking for this view.
		oo = p.cachedNodePods(ctx, nodeName)
		blockMx = false
	case !lsel.Empty() && !client.IsClusterWide(ns):
		// owner->pods drill-in (deployment/statefulset/daemonset/service ->
		// pods), which pins a label selector. Same problem as node->pods: the
		// per-namespace pod informer must LIST & decode EVERY pod in the
		// namespace before the (small) filtered set appears -- ~20s on a busy
		// namespace. Instead do a scoped, server-side labelSelector LIST
		// (RV=0, served from the watch cache), non-blocking + cached.
		oo = p.cachedScopedPods(ctx, ns, lsel)
		blockMx = false
	default:
		oo, err = p.Resource.List(ctx, ns)
		if err != nil {
			return oo, err
		}
	}

	var pmx client.PodsMetricsMap
	if withMx {
		pmx = p.podsMetrics(ctx, ns, blockMx)
	}

	res := make([]runtime.Object, 0, len(oo))
	for _, o := range oo {
		u, ok := o.(*unstructured.Unstructured)
		if !ok {
			return res, fmt.Errorf("expecting *unstructured.Unstructured but got `%T", o)
		}
		fqn := extractFQN(o)
		res = append(res, &render.PodWithMetrics{Raw: u, MX: pmx[fqn]})
	}

	return res, nil
}

// podMxWarming guards a single in-flight background pod-metrics warm so refresh
// ticks don't pile up concurrent cluster-wide metrics LISTs.
var podMxWarming atomic.Bool

// podsMetrics returns the pod metrics map. When block is true it fetches
// synchronously (legacy behavior). When false it only returns already-cached
// metrics and warms the cache in the background, so a slow cluster-wide metrics
// LIST never blocks rendering the (already scoped) pod list.
func (p *Pod) podsMetrics(ctx context.Context, ns string, block bool) client.PodsMetricsMap {
	ms := client.DialMetrics(p.Client())
	if block {
		pmx, _ := ms.FetchPodsMetricsMap(ctx, ns)
		return pmx
	}

	// Fast path: a short deadline still satisfies a cache hit (the cache lookup
	// happens before any network call), but bounds a cold fetch.
	cctx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	if pmx, err := ms.FetchPodsMetricsMap(cctx, ns); err == nil && len(pmx) > 0 {
		return pmx
	}

	// Cold cache: warm it once in the background; this tick renders without
	// metrics and the next refresh picks them up from cache.
	if podMxWarming.CompareAndSwap(false, true) {
		go func() {
			defer podMxWarming.Store(false)
			_, _ = ms.FetchPodsMetricsMap(context.Background(), ns)
		}()
	}

	return nil
}

// nodePodsCache holds the most recently fetched pods per node, plus a
// single-flight flag so refresh ticks don't pile up concurrent fetches.
var nodePodsCache = struct {
	sync.Mutex
	data     map[string][]runtime.Object
	inFlight map[string]bool
}{
	data:     map[string][]runtime.Object{},
	inFlight: map[string]bool{},
}

// PodsNodeLoading reports whether the node's pods are still being fetched for
// the first time (no cached result yet). The UI uses this to keep a loading
// indicator up until data lands.
func PodsNodeLoading(nodeName string) bool {
	nodePodsCache.Lock()
	defer nodePodsCache.Unlock()
	_, ok := nodePodsCache.data[nodeName]

	return !ok
}

// cachedNodePods returns the cached pods for a node and triggers a background
// refresh of that cache. It never blocks on the network, so it is safe to call
// from the synchronous first Watch refresh on the event loop.
func (p *Pod) cachedNodePods(ctx context.Context, nodeName string) []runtime.Object {
	lsel := labels.Everything()
	if sel, ok := ctx.Value(internal.KeyLabels).(labels.Selector); ok {
		lsel = sel
	}

	nodePodsCache.Lock()
	cached := nodePodsCache.data[nodeName]
	if !nodePodsCache.inFlight[nodeName] {
		nodePodsCache.inFlight[nodeName] = true
		go func() {
			oo, err := p.listByNode(context.Background(), nodeName, lsel)
			nodePodsCache.Lock()
			nodePodsCache.inFlight[nodeName] = false
			if err == nil {
				nodePodsCache.data[nodeName] = oo
			}
			nodePodsCache.Unlock()
			if err != nil {
				slog.Error("Unable to list node pods", slogs.ResName, nodeName, slogs.Error, err)
			}
		}()
	}
	nodePodsCache.Unlock()

	return cached
}

// listByNode fetches pods scheduled on a node directly from the API server
// using a server-side fieldSelector, bypassing the cluster-wide pod informer.
func (p *Pod) listByNode(ctx context.Context, nodeName string, lsel labels.Selector) ([]runtime.Object, error) {
	dial, err := p.dynClient()
	if err != nil {
		return nil, err
	}
	cctx, cancel := context.WithTimeout(ctx, p.Client().Config().CallTimeout())
	defer cancel()

	ll, err := dial.Namespace(client.BlankNamespace).List(cctx, metav1.ListOptions{
		FieldSelector: "spec.nodeName=" + nodeName,
		LabelSelector: lsel.String(),
		// ResourceVersion "0" serves the list from the apiserver watch cache
		// (which has a spec.nodeName index) instead of a consistent etcd read
		// that scans every pod in the cluster. Much faster on large clusters;
		// the slight staleness is irrelevant since we re-list each refresh.
		ResourceVersion: "0",
	})
	if err != nil {
		return nil, err
	}
	oo := make([]runtime.Object, len(ll.Items))
	for i := range ll.Items {
		oo[i] = &ll.Items[i]
	}

	return oo, nil
}

// scopedPodsCache holds the most recently fetched pods per (namespace,label
// selector) key, with a single-flight flag so refresh ticks don't pile up
// concurrent fetches. Mirrors nodePodsCache for owner->pods drill-ins.
var scopedPodsCache = struct {
	sync.Mutex
	data     map[string][]runtime.Object
	inFlight map[string]bool
}{
	data:     map[string][]runtime.Object{},
	inFlight: map[string]bool{},
}

// cachedScopedPods returns cached pods for a (namespace,selector) and triggers a
// background server-side labelSelector LIST to refresh that cache. It never
// blocks on the network, so it is safe to call from the synchronous first Watch
// refresh on the tcell event loop.
func (p *Pod) cachedScopedPods(ctx context.Context, ns string, lsel labels.Selector) []runtime.Object {
	key := ns + "\x00" + lsel.String()

	scopedPodsCache.Lock()
	cached := scopedPodsCache.data[key]
	if !scopedPodsCache.inFlight[key] {
		scopedPodsCache.inFlight[key] = true
		go func() {
			oo, err := p.listByLabel(context.Background(), ns, lsel)
			scopedPodsCache.Lock()
			scopedPodsCache.inFlight[key] = false
			if err == nil {
				scopedPodsCache.data[key] = oo
			}
			scopedPodsCache.Unlock()
			if err != nil {
				slog.Error("Unable to list scoped pods", slogs.Namespace, ns, "selector", lsel.String(), slogs.Error, err)
			}
		}()
	}
	scopedPodsCache.Unlock()

	return cached
}

// listByLabel fetches pods matching a label selector in a namespace directly
// from the API server, bypassing the per-namespace pod informer (which must
// list & decode every pod in the namespace before the filtered set appears).
func (p *Pod) listByLabel(ctx context.Context, ns string, lsel labels.Selector) ([]runtime.Object, error) {
	dial, err := p.dynClient()
	if err != nil {
		return nil, err
	}
	cctx, cancel := context.WithTimeout(ctx, p.Client().Config().CallTimeout())
	defer cancel()

	ll, err := dial.Namespace(ns).List(cctx, metav1.ListOptions{
		LabelSelector: lsel.String(),
		// RV=0 serves the list from the apiserver watch cache instead of a
		// consistent etcd read (multi-second on large clusters). Slight
		// staleness is irrelevant since we re-list each refresh tick.
		ResourceVersion: "0",
	})
	if err != nil {
		return nil, err
	}
	oo := make([]runtime.Object, len(ll.Items))
	for i := range ll.Items {
		oo[i] = &ll.Items[i]
	}

	return oo, nil
}

// Logs fetch container logs for a given pod and container.
func (p *Pod) Logs(path string, opts *v1.PodLogOptions) (*restclient.Request, error) {
	ns, n := client.Namespaced(path)
	auth, err := p.Client().CanI(ns, client.NewGVR(client.PodGVR.String()+":log"), n, client.GetAccess)
	if err != nil {
		return nil, err
	}
	if !auth {
		return nil, fmt.Errorf("user is not authorized to view pod logs")
	}

	dial, err := p.Client().DialLogs()
	if err != nil {
		return nil, err
	}

	return dial.CoreV1().Pods(ns).GetLogs(n, opts), nil
}

// Containers returns all container names on pod.
func (p *Pod) Containers(path string, includeInit bool) ([]string, error) {
	pod, err := p.GetInstance(path)
	if err != nil {
		return nil, err
	}

	cc := make([]string, 0, len(pod.Spec.Containers)+len(pod.Spec.InitContainers))
	for i := range pod.Spec.Containers {
		cc = append(cc, pod.Spec.Containers[i].Name)
	}

	if includeInit {
		for i := range pod.Spec.InitContainers {
			cc = append(cc, pod.Spec.InitContainers[i].Name)
		}
	}

	return cc, nil
}

// Pod returns a pod victim by name.
func (*Pod) Pod(fqn string) (string, error) {
	return fqn, nil
}

// GetInstance returns a pod instance.
func (p *Pod) GetInstance(fqn string) (*v1.Pod, error) {
	return fetchPodSpec(p.getFactory(), p.Client(), fqn)
}

// fetchPodSpec reads a pod non-blocking from the informer cache, falling back
// to a direct server-side GET (ResourceVersion=0, served from the apiserver
// watch cache) when the cache is cold. This avoids fac.Get(wait=true), which on
// a cold cache -- e.g. node-scoped drill-ins that intentionally never start the
// pod informer -- blocks the UI up to ~10*defaultWaitTime (~5s) and may error
// with "failed to locate pod", and also avoids spinning up a spurious informer
// + watch just to read a single pod.
// FetchPod returns a pod spec without blocking the UI on a cold informer
// cache (non-blocking cache read + direct server-side GET fallback).
func FetchPod(f Factory, fqn string) (*v1.Pod, error) {
	return fetchPodSpec(f, f.Client(), fqn)
}

func fetchPodSpec(f Factory, c client.Connection, fqn string) (*v1.Pod, error) {
	o, err := cachedOrDirectGet(f, c, client.PodGVR, fqn)
	if err != nil {
		return nil, fmt.Errorf("failed to locate pod %q: %w", fqn, err)
	}
	u, ok := o.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("expecting *unstructured.Unstructured for pod %q but got %T", fqn, o)
	}
	var po v1.Pod
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, &po); err != nil {
		return nil, err
	}

	return &po, nil
}

// TailLogs tails a given container logs.
func (p *Pod) TailLogs(ctx context.Context, opts *LogOptions) ([]LogChan, error) {
	fac, ok := ctx.Value(internal.KeyFactory).(*watch.Factory)
	if !ok {
		return nil, errors.New("no factory in context")
	}
	pop, err := fetchPodSpec(fac, p.Client(), opts.Path)
	if err != nil {
		return nil, err
	}
	po := *pop
	coCounts := len(po.Spec.InitContainers) + len(po.Spec.Containers) + len(po.Spec.EphemeralContainers)
	if coCounts == 1 {
		opts.SingleContainer = true
	}

	outs := make([]LogChan, 0, coCounts)
	if co, ok := GetDefaultContainer(&po.ObjectMeta, &po.Spec); ok && !opts.AllContainers {
		opts.DefaultContainer = co
		return append(outs, tailLogs(ctx, p, opts)), nil
	}
	if opts.HasContainer() && !opts.AllContainers {
		return append(outs, tailLogs(ctx, p, opts)), nil
	}
	for i := range po.Spec.InitContainers {
		cfg := opts.Clone()
		cfg.Container = po.Spec.InitContainers[i].Name
		outs = append(outs, tailLogs(ctx, p, cfg))
	}
	for i := range po.Spec.Containers {
		cfg := opts.Clone()
		cfg.Container = po.Spec.Containers[i].Name
		outs = append(outs, tailLogs(ctx, p, cfg))
	}
	for i := range po.Spec.EphemeralContainers {
		cfg := opts.Clone()
		cfg.Container = po.Spec.EphemeralContainers[i].Name
		outs = append(outs, tailLogs(ctx, p, cfg))
	}

	return outs, nil
}

// ScanSA scans for ServiceAccount refs.
func (p *Pod) ScanSA(_ context.Context, fqn string, wait bool) (Refs, error) {
	ns, n := client.Namespaced(fqn)
	oo, err := p.getFactory().List(p.gvr, ns, wait, labels.Everything())
	if err != nil {
		return nil, err
	}

	refs := make(Refs, 0, len(oo))
	for _, o := range oo {
		var pod v1.Pod
		err = runtime.DefaultUnstructuredConverter.FromUnstructured(o.(*unstructured.Unstructured).Object, &pod)
		if err != nil {
			return nil, errors.New("expecting Deployment resource")
		}
		// Just pick controller less pods...
		if len(pod.OwnerReferences) > 0 {
			continue
		}
		if serviceAccountMatches(pod.Spec.ServiceAccountName, n) {
			refs = append(refs, Ref{
				GVR: p.GVR(),
				FQN: client.FQN(pod.Namespace, pod.Name),
			})
		}
	}

	return refs, nil
}

// Scan scans for cluster resource refs.
func (p *Pod) Scan(_ context.Context, gvr *client.GVR, fqn string, wait bool) (Refs, error) {
	ns, n := client.Namespaced(fqn)
	oo, err := p.getFactory().List(p.gvr, ns, wait, labels.Everything())
	if err != nil {
		return nil, err
	}

	refs := make(Refs, 0, len(oo))
	for _, o := range oo {
		var pod v1.Pod
		err = runtime.DefaultUnstructuredConverter.FromUnstructured(o.(*unstructured.Unstructured).Object, &pod)
		if err != nil {
			return nil, errors.New("expecting Pod resource")
		}
		// Just pick controller less pods...
		if len(pod.OwnerReferences) > 0 {
			continue
		}
		switch gvr {
		case client.CmGVR:
			if !hasConfigMap(&pod.Spec, n) {
				continue
			}
			refs = append(refs, Ref{
				GVR: p.GVR(),
				FQN: client.FQN(pod.Namespace, pod.Name),
			})
		case client.SecGVR:
			found, err := hasSecret(p.Factory, &pod.Spec, pod.Namespace, n, wait)
			if err != nil {
				slog.Warn("Locate secret failed",
					slogs.FQN, fqn,
					slogs.Error, err,
				)
				continue
			}
			if !found {
				continue
			}
			refs = append(refs, Ref{
				GVR: p.GVR(),
				FQN: client.FQN(pod.Namespace, pod.Name),
			})
		case client.PvcGVR:
			if !hasPVC(&pod.Spec, n) {
				continue
			}
			refs = append(refs, Ref{
				GVR: p.GVR(),
				FQN: client.FQN(pod.Namespace, pod.Name),
			})
		case client.PcGVR:
			if !hasPC(&pod.Spec, n) {
				continue
			}
			refs = append(refs, Ref{
				GVR: p.GVR(),
				FQN: client.FQN(pod.Namespace, pod.Name),
			})
		}
	}

	return refs, nil
}

// ----------------------------------------------------------------------------
// Helpers...

func tailLogs(ctx context.Context, logger Logger, opts *LogOptions) LogChan {
	out := make(LogChan, logChannelBuffer)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		podOpts := opts.ToPodLogOptions()

		// Setup exponential backoff following project pattern
		bf := backoff.NewExponentialBackOff()
		bf.InitialInterval = logBackoffInitial
		bf.MaxElapsedTime = 0
		bf.MaxInterval = logBackoffMax / 2
		backoffCtx := backoff.WithContext(bf, ctx)
		delay := logBackoffInitial

		for range logRetryCount {
			req, err := logger.Logs(opts.Path, podOpts)
			if err != nil {
				slog.Error("Log request failed",
					slogs.Container, opts.Info(),
					slogs.Error, err,
				)
				// Check if we should stop retrying based on pod status
				if pod, ok := logger.(*Pod); ok && pod.shouldStopRetrying(opts.Path) {
					slog.Debug("Stopping log retry - pod is terminating or deleted",
						slogs.Container, opts.Info(),
					)
					return
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(delay):
					if delay = backoffCtx.NextBackOff(); delay == backoff.Stop {
						return
					}
				}
				continue
			}

			stream, e := req.Stream(ctx)
			if e != nil {
				slog.Error("Stream logs failed",
					slogs.Error, e,
					slogs.Container, opts.Info(),
				)
				// Check if we should stop retrying based on pod status
				if pod, ok := logger.(*Pod); ok && pod.shouldStopRetrying(opts.Path) {
					slog.Debug("Stopping log retry - pod is terminating or deleted",
						slogs.Container, opts.Info(),
					)
					return
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(delay):
					if delay = backoffCtx.NextBackOff(); delay == backoff.Stop {
						return
					}
				}
				continue
			}

			// Process logs until completion
			result := readLogs(ctx, stream, out, opts)
			switch result {
			case streamEOF:
				slog.Debug("Log stream ended cleanly",
					slogs.Container, opts.Info(),
				)
				return
			case streamError:
				// Check if we should stop retrying based on pod status
				if pod, ok := logger.(*Pod); ok && pod.shouldStopRetrying(opts.Path) {
					slog.Debug("Stopping log retry after stream error - pod is terminating or deleted",
						slogs.Container, opts.Info(),
					)
					return
				}
				slog.Debug("Log stream error, retrying",
					slogs.Container, opts.Info(),
				)
				select {
				case <-ctx.Done():
					return
				case <-time.After(delay):
					if delay = backoffCtx.NextBackOff(); delay == backoff.Stop {
						return
					}
				}
				continue
			case streamCanceled:
				return
			}

			// Reset backoff and delay on successful connection
			bf.Reset()
			delay = logBackoffInitial
		}

		// Out of retries
		out <- opts.ToErrLogItem(fmt.Errorf("failed to maintain log stream after %d retries", logRetryCount))
	}()

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func readLogs(ctx context.Context, stream io.ReadCloser, out chan<- *LogItem, opts *LogOptions) streamResult {
	defer func() {
		if err := stream.Close(); err != nil && !errors.Is(err, io.ErrClosedPipe) {
			slog.Error("Failed to close stream",
				slogs.Container, opts.Info(),
				slogs.Error, err,
			)
		}
	}()

	r := bufio.NewReader(stream)

	for {
		bytes, err := r.ReadBytes('\n')
		if err == nil {
			item := opts.ToLogItem(tview.EscapeBytes(bytes))
			select {
			case <-ctx.Done():
				return streamCanceled
			case out <- item:
			default:
				// Avoid deadlock if consumer is too slow
				slog.Warn("Dropping log line due to slow consumer",
					slogs.Container, opts.Info(),
				)
			}
			continue
		}

		if errors.Is(err, io.EOF) {
			if len(bytes) > 0 {
				// Emit trailing partial line before EOF
				out <- opts.ToLogItem(tview.EscapeBytes(bytes))
			}
			slog.Debug("Log reader reached EOF", slogs.Container, opts.Info())
			out <- opts.ToErrLogItem(fmt.Errorf("stream closed: %w for %s", err, opts.Info()))
			return streamEOF
		}

		// Non-EOF error
		slog.Debug("Log stream error, will retry connection",
			slogs.Container, opts.Info(),
			slogs.Error, fmt.Errorf("stream error: %w for %s", err, opts.Info()),
		)
		// Don't send stream errors to user - they will be retried
		// Only final retry exhaustion message is shown
		return streamError
	}
}

// MetaFQN returns a fully qualified resource name.
func MetaFQN(m *metav1.ObjectMeta) string {
	if m.Namespace == "" {
		return m.Name
	}

	return FQN(m.Namespace, m.Name)
}

// GetPodSpec returns a pod spec given a resource.
func (p *Pod) GetPodSpec(path string) (*v1.PodSpec, error) {
	pod, err := p.GetInstance(path)
	if err != nil {
		return nil, err
	}
	podSpec := pod.Spec

	return &podSpec, nil
}

// SetImages sets container images.
func (p *Pod) SetImages(ctx context.Context, path string, imageSpecs ImageSpecs) error {
	ns, n := client.Namespaced(path)
	auth, err := p.Client().CanI(ns, p.gvr, n, client.PatchAccess)
	if err != nil {
		return err
	}
	if !auth {
		return fmt.Errorf("user is not authorized to patch a deployment")
	}
	manager, isManaged, err := p.isControlled(path)
	if err != nil {
		return err
	}
	if isManaged {
		return fmt.Errorf("unable to set image. This pod is managed by %s. Please set the image on the controller", manager)
	}
	jsonPatch, err := GetJsonPatch(imageSpecs)
	if err != nil {
		return err
	}
	dial, err := p.Client().Dial()
	if err != nil {
		return err
	}
	_, err = dial.CoreV1().Pods(ns).Patch(
		ctx,
		n,
		types.StrategicMergePatchType,
		jsonPatch,
		metav1.PatchOptions{},
	)

	return err
}

func (p *Pod) isControlled(path string) (fqn string, ok bool, err error) {
	pod, err := p.GetInstance(path)
	if err != nil {
		return "", false, err
	}
	references := pod.GetObjectMeta().GetOwnerReferences()
	if len(references) > 0 {
		return fmt.Sprintf("%s/%s", references[0].Kind, references[0].Name), true, nil
	}

	return "", false, nil
}

var toastPhases = sets.New(
	render.PhaseCompleted,
	render.PhasePending,
	render.PhaseCrashLoop,
	render.PhaseError,
	render.PhaseImagePullBackOff,
	render.PhaseContainerStatusUnknown,
	render.PhaseEvicted,
	render.PhaseOOMKilled,
)

func (p *Pod) Sanitize(ctx context.Context, ns string) (int, error) {
	oo, err := p.Resource.List(ctx, ns)
	if err != nil {
		return 0, err
	}

	var count int
	for _, o := range oo {
		u, ok := o.(*unstructured.Unstructured)
		if !ok {
			continue
		}
		var pod v1.Pod
		err = runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, &pod)
		if err != nil {
			continue
		}

		if toastPhases.Has(render.PodStatus(&pod)) {
			// !!BOZO!! Might need to bump timeout otherwise rev limit if too many??
			fqn := client.FQN(pod.Namespace, pod.Name)
			slog.Debug("Sanitizing resource", slogs.FQN, fqn)
			if err := p.Delete(ctx, fqn, nil, 0); err != nil {
				slog.Debug("Aborted! Sanitizer delete failed",
					slogs.FQN, fqn,
					slogs.Count, count,
					slogs.Error, err,
				)
				return count, err
			}
			count++
		}
	}
	slog.Debug("Sanitizer deleted pods", slogs.Count, count)

	return count, nil
}
