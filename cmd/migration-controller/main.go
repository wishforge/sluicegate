package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/wishforge/sluicegate/internal/common"
	"github.com/wishforge/sluicegate/internal/migration"
)

const controllerLeaseResource = "controller"

type controller struct {
	svc      *migration.Service
	store    migration.Store
	log      *common.SLogger
	metrics  *common.Metrics
	holder   string
	leaseTTL time.Duration
}

func main() {
	log := common.NewLogger("migration-controller")
	metrics := &common.Metrics{}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dsn := getenv("DATABASE_URL", "postgres://migration:migration@127.0.0.1:55432/migration?sslmode=disable")
	store, err := migration.OpenPostgresStore(ctx, dsn)
	if err != nil {
		log.Error("startup_failed", "error_code", "POSTGRES_STORE_INIT_FAILED", "error", err.Error())
		os.Exit(1)
	}
	defer store.Close()

	leaseTTL := controllerLeaseTTL()
	holder := getenv("CONTROLLER_ID", common.ID("controller"))
	acquired, err := store.TryAcquireControllerLease(ctx, holder, leaseTTL)
	if err != nil {
		log.Error("controller_lease_acquire_failed", "error", err.Error())
		os.Exit(1)
	}
	if !acquired {
		log.Error("controller_lease_not_acquired", "error_code", "CONTROLLER_ALREADY_ACTIVE", "resource_id", controllerLeaseResource)
		os.Exit(2)
	}
	log.Info("controller_lease_acquired", "resource_id", controllerLeaseResource, "holder_id", holder, "ttl_ms", leaseTTL.Milliseconds())

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	c := &controller{svc: migration.NewService(store, log, metrics), store: store, log: log, metrics: metrics, holder: holder, leaseTTL: leaseTTL}
	go c.renewControllerLease(runCtx, cancel)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", c.health)
	mux.HandleFunc("/readyz", c.health)
	mux.Handle("/metrics", http.HandlerFunc(metrics.Handler))
	mux.HandleFunc("/v1/migrations", c.create)
	mux.HandleFunc("/v1/migrations/", c.migration)
	srv := common.NewHTTPServer(getenv("HTTP_ADDR", ":8080"), mux)
	srv.Handler = common.WrapRequest(mux, log)

	if getenv("RECONCILER_ENABLED", "true") == "true" {
		go c.reconcileLoop(runCtx)
	} else {
		log.Info("reconciler_disabled")
	}
	go func() {
		log.Info("server_started", "addr", srv.Addr, "role", "controller", "store", "postgres", "holder_id", c.holder, "db_max_conns", getenv("DB_MAX_CONNS", "32"))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server_stopped_with_error", "error", err.Error())
			stop()
		}
	}()

	<-runCtx.Done()
	log.Info("server_shutdown", "reason", shutdownReason(runCtx, ctx))
	common.Shutdown(srv)
	_ = store.ReleaseControllerLease(context.Background(), holder)
}

func (c *controller) renewControllerLease(ctx context.Context, cancel context.CancelFunc) {
	ticker := time.NewTicker(c.leaseTTL / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.store.RenewControllerLease(ctx, c.holder, c.leaseTTL); err != nil {
				c.log.Error("controller_lease_renew_failed", "resource_id", controllerLeaseResource, "holder_id", c.holder, "error", err.Error())
				cancel()
				return
			}
		}
	}
}

func (c *controller) health(w http.ResponseWriter, _ *http.Request) {
	body := map[string]any{"status": "ok", "role": "migration-controller", "store": "postgres", "controller_lease": "held", "holder_id": c.holder}
	if ps, ok := c.store.(interface{ PoolStats() migration.DBPoolStats }); ok {
		body["db_pool"] = ps.PoolStats()
	}
	common.JSON(w, 200, body)
}

func (c *controller) create(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		common.Error(w, 405, "METHOD_NOT_ALLOWED", errors.New("method not allowed"))
		return
	}
	var req migration.CreateRequest
	if err := common.DecodeJSON(r, &req); err != nil {
		common.Error(w, 400, "INVALID_REQUEST", err)
		return
	}
	m, replay, err := c.svc.Create(req, r.Header.Get("Idempotency-Key"))
	if err != nil {
		common.Error(w, 400, "CREATE_FAILED", err)
		return
	}
	c.metrics.Actions.Add(1)
	status := 201
	if replay {
		status = 200
	}
	common.JSON(w, status, m)
}

func (c *controller) migration(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/migrations/"), "/")
	if len(parts) < 1 || parts[0] == "" {
		common.Error(w, 404, "NOT_FOUND", errors.New("migration not found"))
		return
	}
	id := parts[0]
	if len(parts) == 1 && r.Method == http.MethodGet {
		m, ok := c.svc.Get(id)
		if !ok {
			common.Error(w, 404, "MIGRATION_NOT_FOUND", errors.New("migration not found"))
			return
		}
		common.JSON(w, 200, m)
		return
	}
	if len(parts) != 2 || r.Method != http.MethodPost {
		common.Error(w, 404, "NOT_FOUND", errors.New("route not found"))
		return
	}
	requestID := common.RequestID(r)
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Minute)
	defer cancel()
	m, err := c.runAction(ctx, id, parts[1], requestID)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			common.Error(w, 404, "MIGRATION_NOT_FOUND", err)
			return
		}
		if strings.HasPrefix(err.Error(), "invalid transition") || strings.Contains(err.Error(), "requires") {
			common.Error(w, 409, "INVALID_STATE", err)
			return
		}
		if strings.Contains(err.Error(), "MIGRATION_BUSY") {
			common.Error(w, 409, "MIGRATION_BUSY", err)
			return
		}
		common.Error(w, 502, "MIGRATION_ACTION_FAILED", err)
		return
	}
	c.metrics.Actions.Add(1)
	common.JSON(w, 200, m)
}

func (c *controller) runAction(ctx context.Context, id, action, requestID string) (migration.Migration, error) {
	acquired, err := c.store.TryAcquireMigrationLease(ctx, id, c.holder, c.leaseTTL)
	if err != nil {
		return migration.Migration{}, err
	}
	if !acquired {
		return migration.Migration{}, errors.New("MIGRATION_BUSY: another controller action holds the migration lease")
	}
	actionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	renewDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(c.leaseTTL / 3)
		defer ticker.Stop()
		defer close(renewDone)
		for {
			select {
			case <-actionCtx.Done():
				return
			case <-ticker.C:
				if err := c.store.RenewMigrationLease(actionCtx, id, c.holder, c.leaseTTL); err != nil {
					c.log.Error("migration_lease_renew_failed", "migration_id", id, "holder_id", c.holder, "error", err.Error())
					cancel()
					return
				}
			}
		}
	}()
	defer func() {
		cancel()
		<-renewDone
		_ = c.store.ReleaseMigrationLease(context.Background(), id, c.holder)
	}()

	switch action {
	case "prepare":
		return c.svc.Prepare(actionCtx, id, requestID)
	case "transfer":
		return c.svc.Transfer(actionCtx, id, requestID)
	case "activate":
		return c.svc.Activate(actionCtx, id, requestID)
	case "commit":
		return c.svc.Commit(actionCtx, id, requestID)
	case "rollback":
		return c.svc.Rollback(actionCtx, id, requestID)
	default:
		return migration.Migration{}, errors.New("unknown action")
	}
}

func (c *controller) reconcileLoop(ctx context.Context) {
	ms := 1000
	if raw := os.Getenv("RECONCILE_INTERVAL_MS"); raw != "" {
		var n int
		if _, err := fmt.Sscanf(raw, "%d", &n); err == nil && n > 0 {
			ms = n
		}
	}
	ticker := time.NewTicker(time.Duration(ms) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rows, err := c.store.ListReconcilable(ctx, 50)
			if err != nil {
				c.log.Warn("reconcile_list_failed", "error", err.Error())
				continue
			}
			for _, m := range rows {
				c.reconcileOne(ctx, m)
			}
		}
	}
}

func (c *controller) reconcileOne(ctx context.Context, m migration.Migration) {
	acquired, err := c.store.TryAcquireMigrationLease(ctx, m.ID, c.holder, c.leaseTTL)
	if err != nil {
		c.log.Warn("reconcile_migration_lease_failed", "migration_id", m.ID, "error", err.Error())
		return
	}
	if !acquired {
		return
	}
	actionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	renewDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(c.leaseTTL / 3)
		defer ticker.Stop()
		defer close(renewDone)
		for {
			select {
			case <-actionCtx.Done():
				return
			case <-ticker.C:
				if err := c.store.RenewMigrationLease(actionCtx, m.ID, c.holder, c.leaseTTL); err != nil {
					c.log.Error("migration_lease_renew_failed", "migration_id", m.ID, "holder_id", c.holder, "error", err.Error())
					cancel()
					return
				}
			}
		}
	}()
	defer func() {
		cancel()
		<-renewDone
		_ = c.store.ReleaseMigrationLease(context.Background(), m.ID, c.holder)
	}()

	requestID := common.ID("reconcile")
	var next string
	switch m.State {
	case migration.StatePending, migration.StateFencing, migration.StatePreparing:
		next = "prepare"
	case migration.StateFrozen, migration.StateTransferring:
		next = "transfer"
	case migration.StateActivating:
		next = "activate"
	case migration.StateActivated, migration.StateCommitting:
		next = "commit"
	case migration.StateRollingBack:
		next = "rollback"
	default:
		return
	}
	c.log.Info("migration_reconcile_attempt", "migration_id", m.ID, "workload_id", m.WorkloadID, "state", m.State, "action", next)
	var actionErr error
	switch next {
	case "prepare":
		_, actionErr = c.svc.Prepare(actionCtx, m.ID, requestID)
	case "transfer":
		_, actionErr = c.svc.Transfer(actionCtx, m.ID, requestID)
	case "activate":
		_, actionErr = c.svc.Activate(actionCtx, m.ID, requestID)
	case "commit":
		_, actionErr = c.svc.Commit(actionCtx, m.ID, requestID)
	default:
		_, actionErr = c.svc.Rollback(actionCtx, m.ID, requestID)
	}
	if actionErr != nil {
		c.log.Warn("migration_reconcile_retryable", "migration_id", m.ID, "action", next, "error", actionErr.Error())
	}
}

func controllerLeaseTTL() time.Duration {
	ms := int64(15000)
	if raw := os.Getenv("CONTROLLER_LEASE_TTL_MS"); raw != "" {
		if _, err := fmt.Sscanf(raw, "%d", &ms); err != nil || ms < 3000 {
			ms = 15000
		}
	}
	return time.Duration(ms) * time.Millisecond
}

func shutdownReason(runCtx, signalCtx context.Context) string {
	if signalCtx.Err() != nil {
		return "signal"
	}
	if runCtx.Err() != nil {
		return runCtx.Err().Error()
	}
	return "context_done"
}

func getenv(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}
