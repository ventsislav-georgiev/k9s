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
}
