package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Yscale-sh/liveresize/internal/advisor"
	"github.com/Yscale-sh/liveresize/internal/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	metricsclient "k8s.io/metrics/pkg/client/clientset/versioned"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	config, err := kubeConfig()
	if err != nil {
		logger.Error("load kubernetes configuration", "error", err)
		os.Exit(1)
	}
	core, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}
	dyn, err := dynamic.NewForConfig(config)
	if err != nil {
		panic(err)
	}
	metrics, err := metricsclient.NewForConfig(config)
	if err != nil {
		panic(err)
	}

	prom := prometheus.New(os.Getenv("PROMETHEUS_URL"), http.DefaultClient)
	a := advisor.New(core, dyn, metrics, prom, logger, advisor.ConfigFromEnv())
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go a.Run(ctx)
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/recommendations", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(a.Snapshot())
	})
	server := &http.Server{Addr: env("LISTEN_ADDR", ":8080"), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { <-ctx.Done(); _ = server.Shutdown(context.Background()) }()
	logger.Info("liveresize started", "listen", server.Addr)
	if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		logger.Error("http server", "error", err)
		os.Exit(1)
	}
}

func kubeConfig() (*rest.Config, error) {
	if c, err := rest.InClusterConfig(); err == nil {
		return c, nil
	}
	if path := os.Getenv("KUBECONFIG"); path != "" {
		return clientcmd.BuildConfigFromFlags("", path)
	}
	return nil, errors.New("no in-cluster configuration and KUBECONFIG is unset")
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
