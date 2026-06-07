//go:build live

// Live integration test against a real cluster. Not run in CI.
// Usage:
//   K9S_CTX=gke_... K9S_NODE=<node> go test -tags live ./internal/dao/ -run TestLiveNodePods -v
package dao

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/derailed/k9s/internal"
	"github.com/derailed/k9s/internal/client"
	"github.com/derailed/k9s/internal/watch"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/cli-runtime/pkg/genericclioptions"
)

func TestLiveNodePods(t *testing.T) {
	ctxName := os.Getenv("K9S_CTX")
	node := os.Getenv("K9S_NODE")
	if ctxName == "" || node == "" {
		t.Skip("set K9S_CTX and K9S_NODE")
	}

	flags := genericclioptions.NewConfigFlags(true)
	flags.Context = &ctxName
	// NewConfigFlags(true) defaults Timeout to "0" (kubectl's "no timeout"),
	// which makes Config.CallTimeout() return 0 and every
	// context.WithTimeout(ctx, CallTimeout()) expire instantly. Real k9s sets
	// this via config.Refine() from APIServerTimeout; mimic that here.
	to := "30s"
	flags.Timeout = &to
	cfg := client.NewConfig(flags)
	conn, err := client.InitConnection(cfg, slog.Default())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	f := watch.NewFactory(conn)
	f.Start(client.BlankNamespace)

	var p Pod
	p.Init(f, client.PodGVR)

	// DEBUG: inspect the rate-limiter config the dynamic client actually gets.
	if rc, e := cfg.RESTConfig(); e == nil {
		t.Logf("RESTConfig QPS=%v Burst=%v Timeout=%v CallTimeout=%v", rc.QPS, rc.Burst, rc.Timeout, cfg.CallTimeout())
	} else {
		t.Logf("RESTConfig err: %v", e)
	}

	// 1) The actual server-side scoped list (what populates the view).
	start := time.Now()
	oo, err := p.listByNode(context.Background(), node, labels.Everything())
	if err != nil {
		t.Fatalf("listByNode: %v", err)
	}
	t.Logf("listByNode: %d pods in %s", len(oo), time.Since(start))

	// 2) Full Pod.List node path (cached + background warm). First call returns
	//    cached (empty) immediately; second after the warm completes.
	ctx := context.WithValue(context.Background(), internal.KeyFields, "spec.nodeName="+node)
	start = time.Now()
	first, _ := p.List(ctx, client.BlankNamespace)
	t.Logf("List #1 (cold, should be instant): %d pods in %s", len(first), time.Since(start))

	time.Sleep(2 * time.Second)
	start = time.Now()
	second, _ := p.List(ctx, client.BlankNamespace)
	t.Logf("List #2 (warm): %d pods in %s", len(second), time.Since(start))

	// 3) Container.fetchPod with a COLD pod informer (the node-scoped drill-in
	//    case). Must be fast via the direct server-side GET fallback, NOT the
	//    ~5s waitForCacheSync*retries that previously froze the UI and errored
	//    with "failed to locate pod".
	if len(oo) == 0 {
		t.Skip("node has no pods to probe containers")
	}
	u, _ := oo[0].(*unstructured.Unstructured)
	fqn := u.GetNamespace() + "/" + u.GetName()

	var c Container
	c.Init(f, client.NewGVR("containers"))

	// Phase A: cold informer cache read (wait=false).
	start = time.Now()
	_, gerr := c.getFactory().Get(client.PodGVR, fqn, false, labels.Everything())
	t.Logf("PhaseA Get(wait=false) cold: err=%v in %s", gerr, time.Since(start))

	// Phase B: Container.fetchPod (non-blocking cache miss -> direct GET RV=0).
	start = time.Now()
	po, err := c.fetchPod(fqn)
	if err != nil {
		t.Fatalf("fetchPod: %v", err)
	}
	t.Logf("PhaseB fetchPod %q: %d containers in %s", fqn, len(po.Spec.Containers), time.Since(start))

	// Phase C: full fetchPod again (cache may now be warm).
	start = time.Now()
	_, _ = c.fetchPod(fqn)
	t.Logf("PhaseC fetchPod: %s", time.Since(start))

	// Phase D: FRESH dynamic client from the same RESTConfig (isolates whether
	// slowness is the config or the shared/cached DynDial client + transport).
	rc, _ := cfg.RESTConfig()
	t.Logf("PhaseD cfg: QPS=%v Burst=%v Timeout=%v WrapTransport=%v Proxy=%v RateLimiter=%v",
		rc.QPS, rc.Burst, rc.Timeout, rc.WrapTransport != nil, rc.Proxy != nil, rc.RateLimiter != nil)
	fresh, derr := dynamic.NewForConfig(rc)
	if derr != nil {
		t.Fatalf("fresh dyn: %v", derr)
	}
	dns, dn := client.Namespaced(fqn)
	gctx, gcancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer gcancel()
	start = time.Now()
	_, derr = fresh.Resource(client.PodGVR.GVR()).Namespace(dns).Get(gctx, dn, metav1.GetOptions{ResourceVersion: "0"})
	t.Logf("PhaseD fresh dyn GET: err=%v in %s", derr, time.Since(start))

	// Phase E: real log path. TailLogs setup must NOT freeze the UI (it runs on
	// the event loop) -- it now uses fetchPodSpec (non-blocking + direct GET)
	// instead of fac.Get(wait=true). Measure setup, then confirm a line flows.
	var lp Pod
	lp.Init(f, client.PodGVR)
	lctx := context.WithValue(context.Background(), internal.KeyFactory, f)
	lctx, lcancel := context.WithCancel(lctx)
	defer lcancel()
	opts := &LogOptions{Path: fqn, Lines: 10, AllContainers: true}
	start = time.Now()
	chans, lerr := lp.TailLogs(lctx, opts)
	t.Logf("PhaseE TailLogs setup: %d streams, err=%v in %s", len(chans), lerr, time.Since(start))
	if lerr == nil && len(chans) > 0 {
		select {
		case it := <-chans[0]:
			if it != nil {
				t.Logf("PhaseE first log line: %d bytes", len(it.Bytes))
			}
		case <-time.After(10 * time.Second):
			t.Logf("PhaseE: no log line within 10s (container may be quiet)")
		}
	}

	// Phase F: Describe (kubectl describe.Describer -> its own clientset built
	// from Config().Flags(), + RESTMapper discovery + Events fetch).
	start = time.Now()
	desc, ferr := Describe(conn, client.PodGVR, fqn)
	t.Logf("PhaseF Describe: %d bytes, err=%v in %s", len(desc), ferr, time.Since(start))
	start = time.Now()
	desc2, _ := Describe(conn, client.PodGVR, fqn)
	t.Logf("PhaseF Describe #2 (warm mapper?): %d bytes in %s", len(desc2), time.Since(start))

	// Phase G: YAML path (Generic.Get -> dynamic GET, no RV=0).
	start = time.Now()
	_, gerr2 := lp.ToYAML(fqn, false)
	t.Logf("PhaseG ToYAML: err=%v in %s", gerr2, time.Since(start))
}
