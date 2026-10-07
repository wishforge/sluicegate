package agent

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/wishforge/sluicegate/internal/common"
)

// Does handleInit write s.states[id] under a per-id lock while other ids
// write the same map concurrently? If two different ids land on different
// stripes, the map write itself is unsynchronized.
func TestInitConcurrentMapWrite(t *testing.T) {
	t.Parallel()

	target, err := NewTargetServer(t.TempDir(), common.NewLogger("maprace"), &common.Metrics{})
	if err != nil {
		t.Fatal(err)
	}

	const n = 64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("race%d", i)
			body := `{"workload_id":"w","files":[{"path":"d.bin","size":10,"sha256":"` +
				"0000000000000000000000000000000000000000000000000000000000000000" +
				`","mode":384,"chunk_size":5,"chunk_count":2}]}`
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/v1/migrations/"+id+"/init", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			target.Handler().ServeHTTP(rec, req)
			if rec.Code/100 != 2 {
				t.Errorf("%s: status=%d body=%s", id, rec.Code, rec.Body.String())
			}
		}(i)
	}
	wg.Wait()
}
