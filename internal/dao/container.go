// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of K9s

package dao

import (
	"context"
	"fmt"
	"strconv"

	"github.com/derailed/k9s/internal"
	"github.com/derailed/k9s/internal/client"
	"github.com/derailed/k9s/internal/render"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	mv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
)

var (
	_ Accessor = (*Container)(nil)
	_ Loggable = (*Container)(nil)
)

const (
	initIDX = "I"
	mainIDX = "M"
	ephIDX  = "E"
)

// Container represents a pod's container dao.
type Container struct {
	NonResource
}

// List returns a collection of containers.
func (c *Container) List(ctx context.Context, _ string) ([]runtime.Object, error) {
	fqn, ok := ctx.Value(internal.KeyPath).(string)
	if !ok {
		return nil, fmt.Errorf("no context path for %q", c.gvr)
	}

	var (
		cmx client.ContainersMetrics
		err error
	)
	if withMx, ok := ctx.Value(internal.KeyWithMetrics).(bool); ok && withMx {
		cmx, _ = client.DialMetrics(c.Client()).FetchContainersMetrics(ctx, fqn)
	}

	po, err := c.fetchPod(fqn)
	if err != nil {
		return nil, err
	}
	res := make([]runtime.Object, 0, len(po.Spec.InitContainers)+len(po.Spec.Containers)+len(po.Spec.EphemeralContainers))
	for i := range po.Spec.InitContainers {
		res = append(res, makeContainerRes(
			initIDX,
			i,
			&(po.Spec.InitContainers[i]),
			po,
			cmx[po.Spec.InitContainers[i].Name]),
		)
	}
	for i := range po.Spec.Containers {
		res = append(res, makeContainerRes(
			mainIDX,
			i,
			&(po.Spec.Containers[i]),
			po,
			cmx[po.Spec.Containers[i].Name]),
		)
	}
	for i := range po.Spec.EphemeralContainers {
		co := v1.Container(po.Spec.EphemeralContainers[i].EphemeralContainerCommon)
		res = append(res, makeContainerRes(
			ephIDX,
			i,
			&co,
			po,
			cmx[co.Name]),
		)
	}

	return res, nil
}

// TailLogs tails a given container logs.
func (c *Container) TailLogs(ctx context.Context, opts *LogOptions) ([]LogChan, error) {
	po := Pod{}
	po.Init(c.Factory, client.PodGVR)

	return po.TailLogs(ctx, opts)
}

// ----------------------------------------------------------------------------
// Helpers...

func makeContainerRes(kind string, idx int, co *v1.Container, po *v1.Pod, cmx *mv1beta1.ContainerMetrics) render.ContainerRes {
	return render.ContainerRes{
		Idx:       kind + strconv.Itoa(idx+1),
		Container: co,
		Status:    getContainerStatus(kind, co.Name, &po.Status),
		MX:        cmx,
		Age:       po.GetCreationTimestamp(),
	}
}

func getContainerStatus(kind, name string, status *v1.PodStatus) *v1.ContainerStatus {
	switch kind {
	case mainIDX:
		for i := range status.ContainerStatuses {
			if status.ContainerStatuses[i].Name == name {
				return &status.ContainerStatuses[i]
			}
		}
	case initIDX:
		for i := range status.InitContainerStatuses {
			if status.InitContainerStatuses[i].Name == name {
				return &status.InitContainerStatuses[i]
			}
		}
	case ephIDX:
		for i := range status.EphemeralContainerStatuses {
			if status.EphemeralContainerStatuses[i].Name == name {
				return &status.EphemeralContainerStatuses[i]
			}
		}
	}

	return nil
}

func (c *Container) fetchPod(fqn string) (*v1.Pod, error) {
	// Non-blocking cache read (wait=false). When the pod informer is warm this
	// is instant. When it is cold -- e.g. drilled in from a node-scoped pod
	// list, which intentionally does NOT start the cluster-wide pod informer --
	// we must NOT wait=true: getWithRetries would block the UI for up to
	// 10*defaultWaitTime (~5s) and then fail with "failed to locate pod",
	// showing a black screen. Fall back to a direct server-side GET instead.
	o, err := c.getFactory().Get(client.PodGVR, fqn, false, labels.Everything())
	if err != nil || o == nil {
		return c.getPodDirect(fqn)
	}
	u, ok := o.(*unstructured.Unstructured)
	if !ok {
		return c.getPodDirect(fqn)
	}
	var po v1.Pod
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, &po); err != nil {
		return nil, err
	}
	return &po, nil
}

// getPodDirect fetches a pod via a single server-side GET, bypassing the
// informer cache. Used when the cache is cold so the container view does not
// freeze the UI waiting for a cache sync that may never complete on a large
// cluster.
func (c *Container) getPodDirect(fqn string) (*v1.Pod, error) {
	ns, n := client.Namespaced(fqn)
	dial, err := c.Client().DynDial()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.Client().Config().CallTimeout())
	defer cancel()

	// ResourceVersion "0" serves the GET from the apiserver watch cache instead
	// of a consistent etcd quorum read, which is multi-second on large/loaded
	// clusters. Staleness is irrelevant for rendering a container list.
	o, err := dial.Resource(client.PodGVR.GVR()).Namespace(ns).Get(ctx, n, metav1.GetOptions{ResourceVersion: "0"})
	if err != nil {
		return nil, fmt.Errorf("failed to locate pod %q: %w", fqn, err)
	}
	var po v1.Pod
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(o.Object, &po); err != nil {
		return nil, err
	}
	return &po, nil
}
