// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of K9s

package render_test

import (
	"testing"

	"github.com/derailed/k9s/internal/model1"
	"github.com/derailed/k9s/internal/render"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	mv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
)

func TestNodeRender(t *testing.T) {
	pom := render.NodeWithMetrics{
		Raw: load(t, "no"),
		MX:  makeNodeMX("n1", "10m", "20Mi"),
	}

	var no render.Node
	r := model1.NewRow(14)
	err := no.Render(&pom, "", &r)
	require.NoError(t, err)

	assert.Equal(t, "minikube", r.ID)
	e := model1.Fields{"minikube", "Ready", "master", "amd64", "0", "v1.15.2", "Buildroot 2018.05.3", "4.15.0", "192.168.64.107", "<none>", "0", "10", "4000", "0", "20", "7874", "0", "n/a", "n/a"}
	assert.Equal(t, e, r.Fields[:19])
}

func TestNodeRenderCapacityBuffer(t *testing.T) {
	uu := map[string]struct {
		suspended, standby, cordoned bool
		e                            string
	}{
		"suspended-standby": {suspended: true, standby: true, cordoned: true, e: "Suspended,Standby"},
		"resumed-standby":   {standby: true, e: "Ready,Standby"},
		"plain-cordoned":    {cordoned: true, e: "Ready,SchedulingDisabled"},
		"plain":             {e: "Ready"},
	}

	for k, u := range uu {
		t.Run(k, func(t *testing.T) {
			node := v1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "n1"},
				Spec:       v1.NodeSpec{Unschedulable: u.cordoned},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{{Type: v1.NodeReady, Status: v1.ConditionTrue}},
				},
			}
			if u.standby {
				node.Labels = map[string]string{"buffer.gke.io/standby-capacity-node": "true"}
			}
			if u.suspended {
				node.Status.Conditions = []v1.NodeCondition{
					{Type: "Suspended", Status: v1.ConditionTrue, Reason: "CapacityBuffer"},
					{Type: v1.NodeReady, Status: v1.ConditionUnknown},
				}
			}
			o, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&node)
			require.NoError(t, err)

			var no render.Node
			r := model1.NewRow(14)
			require.NoError(t, no.Render(&render.NodeWithMetrics{Raw: &unstructured.Unstructured{Object: o}}, "", &r))
			assert.Equal(t, u.e, r.Fields[1])
		})
	}
}

func BenchmarkNodeRender(b *testing.B) {
	var (
		no  render.Node
		r   = model1.NewRow(14)
		pom = render.NodeWithMetrics{
			Raw: load(b, "no"),
			MX:  makeNodeMX("n1", "10m", "10Mi"),
		}
	)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		_ = no.Render(&pom, "", &r)
	}
}

// ----------------------------------------------------------------------------
// Helpers...

func makeNodeMX(name, cpu, mem string) *mv1beta1.NodeMetrics {
	return &mv1beta1.NodeMetrics{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
		Usage: makeRes(cpu, mem),
	}
}
