package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"telemetry-fabric/services/chain-crawler-go/internal/adapters/mock"
	"telemetry-fabric/services/chain-crawler-go/internal/checkpoint"
	"telemetry-fabric/services/chain-crawler-go/internal/config"
	"telemetry-fabric/services/chain-crawler-go/internal/exporter"
	"telemetry-fabric/services/chain-crawler-go/internal/filter"
	"telemetry-fabric/services/chain-crawler-go/internal/limiter"
	"telemetry-fabric/services/chain-crawler-go/internal/metrics"
	"telemetry-fabric/services/chain-crawler-go/internal/rpcpool"
	"telemetry-fabric/services/chain-crawler-go/internal/scheduler"
)

const (
	exitOK              = 0
	exitConfigError     = 1
	exitRuntimeError    = 1
	exitShutdownTimeout = 2
)

func main() {
	os.Exit(run())
}

func run() int {
	var configPath string
	var exporterType string
	var exporterFile string
	var checkpointFile string
	var listenAddr string
	var runOnce bool
	var checkConfig bool

	flag.StringVar(&configPath, "config", "", "optional JSON config path")
	flag.StringVar(&exporterType, "exporter", "", "event exporter: stdout or file")
	flag.StringVar(&exporterFile, "export-file", "", "file exporter JSONL path")
	flag.StringVar(&checkpointFile, "checkpoint-file", "", "durable checkpoint JSON path")
	flag.StringVar(&listenAddr, "listen", "", "health and metrics listen address")
	flag.BoolVar(&runOnce, "run-once", false, "process one scheduler cycle and exit")
	flag.BoolVar(&checkConfig, "check-config", false, "validate configuration and exit without starting the crawler")
	flag.Parse()

	cfg, err := config.Load(configPath)
	if err != nil {
		log.Printf("load config: %v", err)
		return exitConfigError
	}
	if exporterType != "" {
		cfg.Exporter.Type = exporterType
	}
	if exporterFile != "" {
		cfg.Exporter.FilePath = exporterFile
	}
	if checkpointFile != "" {
		cfg.CheckpointFile = checkpointFile
	}
	if listenAddr != "" {
		cfg.HTTPListenAddr = listenAddr
	}
	if err := config.Validate(cfg); err != nil {
		log.Printf("validate config: %v", err)
		return exitConfigError
	}
	if checkConfig {
		_ = json.NewEncoder(os.Stdout).Encode(config.Summary(cfg))
		return exitOK
	}

	registry := metrics.NewRegistry()
	health := metrics.NewHealthState()

	exp, err := buildExporter(cfg, registry)
	if err != nil {
		log.Printf("build exporter: %v", err)
		return exitConfigError
	}
	exporterCtx, cancelExporter := context.WithCancel(context.Background())
	defer cancelExporter()
	if starter, ok := exp.(interface{ Start(context.Context) }); ok {
		starter.Start(exporterCtx)
	}
	var closeExporter sync.Once
	closeExp := func(timeout time.Duration) {
		closeExporter.Do(func() {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			if err := exp.Close(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("close exporter: %v", err)
			}
		})
	}
	defer closeExp(5 * time.Second)

	mux := http.NewServeMux()
	metrics.RegisterHandlers(mux, registry, health)
	server := &http.Server{
		Addr:              cfg.HTTPListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		log.Printf("crawler health and metrics listening on http://%s", cfg.HTTPListenAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
			return
		}
		serverErr <- nil
	}()

	checkpointStore, err := checkpoint.OpenFileStore(cfg.CheckpointFile)
	if err != nil {
		log.Printf("open checkpoint store: %v", err)
		shutdownServer(server)
		closeExp(5 * time.Second)
		return exitRuntimeError
	}
	chainAdapter := mock.New(mock.Config{
		Chain:         cfg.Chain,
		Network:       cfg.Network,
		ChainID:       cfg.ChainID,
		StartHeight:   cfg.StartHeight,
		FinalityDepth: 6,
		HeadInterval:  cfg.PollInterval,
		SourceRPC:     firstEndpointID(cfg),
	})

	retryPolicy := rpcpool.RetryPolicy{
		Attempts:       cfg.Retry.Attempts,
		InitialBackoff: time.Duration(cfg.Retry.InitialBackoffMS) * time.Millisecond,
		MaxBackoff:     time.Duration(cfg.Retry.MaxBackoffMS) * time.Millisecond,
		RetryBudget:    cfg.Retry.RetryBudget,
		Jitter:         float64(cfg.Retry.JitterPercent) / 100,
	}

	crawler := scheduler.New(
		chainAdapter,
		checkpointStore,
		exp,
		filter.AllowAll{},
		limiter.New(cfg.RateLimitRPS),
		retryPolicy,
		registry,
		scheduler.Config{
			StartHeight:  cfg.StartHeight,
			BatchSize:    cfg.BatchSize,
			PollInterval: cfg.PollInterval,
		},
	)

	health.AddCheck("config", func(context.Context) error {
		return config.Validate(cfg)
	})
	health.AddCheck("adapter", func(ctx context.Context) error {
		_, err := chainAdapter.HealthCheck(ctx)
		return err
	})
	health.AddCheck("exporter", func(ctx context.Context) error {
		if checker, ok := exp.(interface{ HealthCheck(context.Context) error }); ok {
			return checker.HealthCheck(ctx)
		}
		return nil
	})
	health.AddCheck("checkpoint_store", func(ctx context.Context) error {
		_, _, err := checkpointStore.Load(ctx, cfg.Chain, cfg.Network)
		return err
	})

	startupCtx, cancelStartup := context.WithTimeout(context.Background(), 5*time.Second)
	if _, err := chainAdapter.HealthCheck(startupCtx); err != nil {
		cancelStartup()
		log.Printf("mock adapter health check failed: %v", err)
		shutdownServer(server)
		closeExp(5 * time.Second)
		return exitRuntimeError
	}
	cancelStartup()
	health.SetReady(true)

	if runOnce {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := crawler.RunOnce(ctx); err != nil {
			log.Printf("run once: %v", err)
			shutdownServer(server)
			closeExp(10 * time.Second)
			return exitRuntimeError
		}
		shutdownServer(server)
		closeExp(10 * time.Second)
		return exitOK
	}

	crawlerCtx, cancelCrawler := context.WithCancel(context.Background())
	defer cancelCrawler()

	crawlerErr := make(chan error, 1)
	go func() {
		crawlerErr <- crawler.Run(crawlerCtx)
	}()

	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)

	select {
	case err := <-serverErr:
		if err != nil {
			log.Printf("http server: %v", err)
			cancelCrawler()
			closeExp(5 * time.Second)
			return exitRuntimeError
		}
	case err := <-crawlerErr:
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, scheduler.ErrStopped) {
			log.Printf("crawler: %v", err)
			shutdownServer(server)
			closeExp(5 * time.Second)
			return exitRuntimeError
		}
	case sig := <-signals:
		log.Printf("received %s, starting graceful shutdown", sig)
		health.SetReady(false)
		crawler.Stop()
		if !waitCrawler(crawlerErr, 20*time.Second) {
			log.Printf("crawler graceful shutdown timed out")
			cancelCrawler()
			shutdownServer(server)
			closeExp(5 * time.Second)
			return exitShutdownTimeout
		}
	}

	health.SetReady(false)
	if err := persistCheckpoint(context.Background(), checkpointStore, cfg.Chain, cfg.Network); err != nil {
		log.Printf("persist checkpoint during shutdown: %v", err)
	}
	shutdownServer(server)
	closeExp(15 * time.Second)
	return exitOK
}

func buildExporter(cfg config.Config, registry *metrics.Registry) (exporter.Exporter, error) {
	var inner exporter.Exporter
	switch cfg.Exporter.Type {
	case "stdout":
		inner = exporter.NewStdoutExporter()
	case "file":
		fileExporter, err := exporter.NewFileExporter(cfg.Exporter.FilePath)
		if err != nil {
			return nil, err
		}
		inner = fileExporter
	default:
		return nil, errors.New("unsupported exporter type: " + cfg.Exporter.Type)
	}

	return exporter.NewBoundedExporter(inner, exporter.BoundedOptions{
		Capacity: cfg.Exporter.QueueSize,
		Policy:   exporter.BackpressurePolicy(cfg.Exporter.BackpressurePolicy),
		Metrics:  registry,
	}), nil
}

func firstEndpointID(cfg config.Config) string {
	if len(cfg.RPC.Endpoints) == 0 || cfg.RPC.Endpoints[0].ID == "" {
		return "mock-rpc-1"
	}
	return cfg.RPC.Endpoints[0].ID
}

func shutdownServer(server *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("shutdown http server: %v", err)
	}
}

func waitCrawler(crawlerErr <-chan error, timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case err := <-crawlerErr:
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, scheduler.ErrStopped) {
			log.Printf("crawler stopped with error: %v", err)
		}
		return true
	case <-timer.C:
		return false
	}
}

func persistCheckpoint(ctx context.Context, store checkpoint.Store, chain string, network string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	cp, ok, err := store.Load(ctx, chain, network)
	if err != nil || !ok {
		return err
	}
	return store.Save(ctx, cp)
}
