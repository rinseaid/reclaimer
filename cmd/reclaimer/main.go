package main

import (
	"embed"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/rinseaid/reclaimer/internal/api"
	"github.com/rinseaid/reclaimer/internal/config"
	"github.com/rinseaid/reclaimer/internal/database"
	"github.com/rinseaid/reclaimer/internal/orchestrator"
	"github.com/rinseaid/reclaimer/internal/scheduler"
	"github.com/rinseaid/reclaimer/internal/store"
	"github.com/rinseaid/reclaimer/internal/viewer"
	"github.com/rinseaid/reclaimer/internal/web"
)

//go:embed all:static
var staticFS embed.FS

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	slog.Info("starting Reclaimer")

	db, err := database.Open()
	if err != nil {
		slog.Error("failed to open database", "error", err)
		os.Exit(1)
	}
	if err := db.Init(); err != nil {
		slog.Error("failed to initialize database", "error", err)
		os.Exit(1)
	}

	cfg := config.New(db)
	if err := cfg.Init(); err != nil {
		slog.Error("failed to initialize config", "error", err)
		os.Exit(1)
	}

	st := store.New(db)

	orch := &orchestrator.Orchestrator{
		Store:  st,
		Config: cfg,
		DB:     db,
	}

	sched := scheduler.New(db)

	hour := cfg.GetInt("schedule_hour")
	minute := cfg.GetInt("schedule_minute")
	cronSpec := fmt.Sprintf("%d %d * * *", minute, hour)
	sched.AddFunc("nightly_run", cronSpec, func() {
		slog.Info("running scheduled orchestrator")
		if err := orch.Run(false, ""); err != nil {
			slog.Error("scheduled run failed", "error", err)
		}
	})

	syncHours := cfg.GetInt("user_sync_interval_hours")
	if syncHours <= 0 {
		syncHours = 6
	}
	syncSpec := fmt.Sprintf("@every %dh", syncHours)
	sched.AddFunc("periodic_user_sync", syncSpec, func() {
		slog.Info("running periodic user sync")
		if err := orch.SyncUsers(); err != nil {
			slog.Error("user sync failed", "error", err)
		}
	})

	sched.ReloadPerRuleSchedules(func(ruleName string) {
		slog.Info("running scheduled rule", "rule", ruleName)
		if err := orch.Run(false, ruleName); err != nil {
			slog.Error("per-rule run failed", "rule", ruleName, "error", err)
		}
	})

	sched.Start()
	defer sched.Stop()

	slog.Info("scheduled nightly run", "hour", hour, "minute", minute)
	slog.Info("scheduled periodic user sync", "hours", syncHours)

	go func() {
		slog.Info("running initial user sync on startup")
		if err := orch.SyncUsers(); err != nil {
			slog.Error("initial user sync failed", "error", err)
		}
	}()

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Compress(5))

	apiServer := &api.Server{
		Store:        st,
		Config:       cfg,
		DB:           db,
		Orchestrator: orch,
	}
	r.Mount("/api", apiServer.Routes())

	tmplDir := os.Getenv("TEMPLATE_DIR")
	if tmplDir == "" {
		tmplDir = "/app/templates"
	}

	viewerServer := &viewer.Server{
		Store:       st,
		Config:      cfg,
		DB:          db,
		TemplateDir: tmplDir,
	}
	r.Mount("/leaving", viewerServer.Routes())
	r.Get("/keep/{token}", viewerServer.HandleMagicKeep)

	sched.AddFunc("viewer_session_cleanup", "@every 1h", func() {
		viewerServer.CleanupSessions()
	})
	webServer := &web.Server{TemplateDir: tmplDir}
	webServer.Routes(r)

	staticSub, err := fs.Sub(staticFS, "static")
	if err == nil {
		r.Handle("/static/*", http.StripPrefix("/static/", http.FileServer(http.FS(staticSub))))
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	addr := ":" + port

	slog.Info("listening", "addr", addr)

	srv := &http.Server{Addr: addr, Handler: r}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("shutting down")
}
