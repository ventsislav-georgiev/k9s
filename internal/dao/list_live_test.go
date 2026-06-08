//go:build live

// Live integration test against a real cluster. Not run in CI.
// Usage:
//   K9S_CTX=gke_... K9S_NS=production K9S_LABEL=app=foo \
//     go test -tags live ./internal/dao/ -run TestLiveListPerf -v
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
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/cli-runtime/pkg/genericclioptions"
)

func TestLiveListPerf(t *testing.T) {
	ctxName := os.Getenv("K9S_CTX")
	if ctxName == "" {
		t.Skip("set K9S_CTX")
	}
	ns := os.Getenv("K9S_NS")
	if ns == "" {
		ns = "production"
	}
	lblStr := os.Getenv("K9S_LABEL") // e.g. app=account-data-high-worker

	flags := genericclioptions.NewConfigFlags(true)
	flags.Context = &ctxName
	to := "30s"
	flags.Timeout = &to
	cfg := client.NewConfig(flags)
	conn, err := client.InitConnection(cfg, slog.Default())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if rc, e := cfg.RESTConfig(); e == nil {
		t.Logf("RESTConfig QPS=%v Burst=%v NextProtos=%v", rc.QPS, rc.Burst, rc.NextProtos)
	}

	f := watch.NewFactory(conn)
	f.Start(client.BlankNamespace)

	// --- NEW dao path: Deployment.List with a COLD informer must paint fast
	//     via the direct-list fallback instead of waiting ~17s for the sync. ---
	{
		var dp Deployment
		dp.Init(f, client.DpGVR)
		dctx := context.Background()
		s := time.Now()
		c1, e1 := dp.List(dctx, client.BlankNamespace)
		t.Logf("Z) Deployment.List #1 (cold, instant cached): %d, err=%v in %s", len(c1), e1, time.Since(s))
		time.Sleep(4 * time.Second) // let the background direct list land
		s = time.Now()
		c2, _ := dp.List(dctx, client.BlankNamespace)
		t.Logf("Z) Deployment.List #2 (direct-list warm, informer still cold): %d in %s", len(c2), time.Since(s))
	}

	// --- Deployment list path (the resource's own informer) ---
	// A) factory.List cluster-wide, wait=true (full informer initial sync).
	start := time.Now()
	dd, derr := f.List(client.DpGVR, client.BlankNamespace, true, labels.Everything())
	t.Logf("A) factory.List deploy ALL wait=true: %d, err=%v in %s", len(dd), derr, time.Since(start))

	// B) poll until the informer reports rows -- measures REAL sync latency.
	bstart := time.Now()
	for i := 0; i < 60; i++ {
		dd2, _ := f.List(client.DpGVR, client.BlankNamespace, false, labels.Everything())
		synced, _ := f.HasSynced(client.DpGVR, client.BlankNamespace)
		if len(dd2) > 0 {
			t.Logf("B) informer deploy ALL got %d rows (synced=%v) after %s", len(dd2), synced, time.Since(bstart))
			break
		}
		if i == 59 {
			t.Logf("B) informer deploy ALL STILL 0 (synced=%v) after %s", synced, time.Since(bstart))
		}
		time.Sleep(500 * time.Millisecond)
	}

	// C) direct dynamic List deploy ALL RV=0 (watch cache).
	dial, _ := conn.DynDial()
	cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer ccancel()
	start = time.Now()
	ll, lerr := dial.Resource(client.DpGVR.GVR()).List(cctx, metav1.ListOptions{ResourceVersion: "0"})
	n := 0
	if ll != nil {
		n = len(ll.Items)
	}
	t.Logf("C) direct dyn List deploy ALL RV=0: %d, err=%v in %s", n, lerr, time.Since(start))

	// D) factory.List deploy in one namespace, wait=true.
	start = time.Now()
	dn, _ := f.List(client.DpGVR, ns, true, labels.Everything())
	t.Logf("D) factory.List deploy ns=%s wait=true: %d in %s", ns, len(dn), time.Since(start))

	// --- Pod scoped-by-label path (deployment->pods drill-in) ---
	if lblStr != "" {
		sel, perr := labels.Parse(lblStr)
		if perr != nil {
			t.Fatalf("bad label %q: %v", lblStr, perr)
		}
		var p Pod
		p.Init(f, client.PodGVR)

		// E) direct server-side labelSelector list (what now backs the drill-in).
		start = time.Now()
		oo, e := p.listByLabel(context.Background(), ns, sel)
		t.Logf("E) listByLabel ns=%s %q: %d, err=%v in %s", ns, lblStr, len(oo), e, time.Since(start))

		// F) Pod.List via the scoped path: cold (instant cached nil) then warm.
		lctx := context.WithValue(context.Background(), internal.KeyLabels, sel)
		start = time.Now()
		c1, _ := p.List(lctx, ns)
		t.Logf("F) Pod.List #1 (cold, instant): %d in %s", len(c1), time.Since(start))
		time.Sleep(2 * time.Second)
		start = time.Now()
		c2, _ := p.List(lctx, ns)
		t.Logf("F) Pod.List #2 (warm): %d in %s", len(c2), time.Since(start))

		// G) baseline: factory pod informer for the whole namespace, wait=true
		//    (the OLD drill-in path -- expect this to be the slow one).
		start = time.Now()
		allp, _ := f.List(client.PodGVR, ns, true, sel)
		t.Logf("G) factory.List pods ns=%s wait=true (OLD path): %d in %s", ns, len(allp), time.Since(start))
	}
}
