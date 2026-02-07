package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"k8s-log-alerter-zulip/internal"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Config represents the application configuration
type Config struct {
	Zulip internal.ZulipConfig      `json:"zulip"`
	Rules map[string]*internal.Rule `json:"rules"`
}

// loadConfig loads a configuration file
func loadConfig(path string) (*Config, error) {
	f, err := os.Open(path) // #nosec G304
	if err != nil {
		return nil, err
	}
	defer f.Close() //nolint:errcheck

	var cfg Config
	decoder := json.NewDecoder(f)
	if err := decoder.Decode(&cfg); err != nil {
		return nil, err
	}

	for name, rule := range cfg.Rules {
		if err := rule.Init(name); err != nil {
			return nil, fmt.Errorf("invalid rule %s: %w", name, err)
		}
	}

	if len(cfg.Rules) == 0 {
		return nil, fmt.Errorf("no rules defined")
	}

	return &cfg, nil
}

// k8sClient creates a client from an external or internal cluster configuration
func k8sClient() (*kubernetes.Clientset, string, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		loadingRules,
		&clientcmd.ConfigOverrides{},
	)

	config, err := kubeConfig.ClientConfig()
	var namespace string

	if err != nil {
		log.Print("Attempting in-cluster config")
		config, err = rest.InClusterConfig()
		if err != nil {
			return nil, "", fmt.Errorf("failed to load kubernetes config: %w", err)
		}
		if data, err := os.ReadFile(
			"/var/run/secrets/kubernetes.io/serviceaccount/namespace",
		); err == nil {
			namespace = strings.TrimSpace(string(data))
		}
	} else {
		namespace, _, _ = kubeConfig.Namespace()
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, "", err
	}

	return clientset, namespace, nil
}

type logWriter struct{}

// Write is a custom logger that outputs the timestamp as ISO8601 UTC
func (writer logWriter) Write(bytes []byte) (int, error) {
	return fmt.Fprintf(
		os.Stderr,
		"%s %s",
		time.Now().UTC().Format("2006-01-02T15:04:05Z"),
		string(bytes),
	)
}

func main() {
	log.SetFlags(0)
	log.SetOutput(new(logWriter))

	configPath := flag.String("c", "", "Path to configuration file")

	namespace := flag.String("n", "", "Namespace")

	listenAddr := flag.String(
		"metrics",
		":8081",
		"Address to listen on for health checks and metrics",
	)

	flag.StringVar(listenAddr, "healthcheck", ":8081", "Alias for -metrics")

	flag.Parse()

	if *configPath == "" {
		flag.Usage()
		os.Exit(1)
	}

	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config %s: %v", *configPath, err)
	}

	zulipClient, err := internal.NewZulipClient(&cfg.Zulip)
	if err != nil {
		log.Fatalf("%v", err)
	}

	clientset, currentNamespace, err := k8sClient()
	if err != nil {
		log.Fatalf("Failed to create Kubernetes client: %v", err)
	}

	// Check we can connect to the server
	version, err := clientset.Discovery().ServerVersion()
	if err != nil {
		log.Fatalf("Failed to connect to Kubernetes API: %v", err)
	}
	log.Printf("Connected to Kubernetes API %s", version)

	if *namespace == "" {
		*namespace = currentNamespace
	}

	log.Printf("Using namespace: %s", *namespace)

	health := &internal.HealthChecker{}

	// https://prometheus.io/docs/guides/go-application/
	reg := prometheus.NewRegistry()
	const promNamespace = "k8slogalerter"
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{
			Namespace: promNamespace,
		}),
	)
	metrics := internal.NewLogwatchMetrics(reg, promNamespace)

	// We've validated that we can connect to the K8s API so now we'll report on whether
	// any of the rule watchers have failed
	go func() {
		srv := &http.Server{
			Addr:         *listenAddr,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 10 * time.Second,
		}
		http.HandleFunc("/healthz", health.ServeHTTP)
		http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
		log.Printf("Starting healthcheck/metrics server on %s", *listenAddr)
		if err := srv.ListenAndServe(); err != nil {
			log.Fatalf("Healthcheck/metrics server failed: %v", err)
		}
	}()

	// Start watchers, handle graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	g, gCtx := errgroup.WithContext(ctx)

	for _, rule := range cfg.Rules {
		r := rule
		g.Go(func() error {
			if err := internal.RunWatcher(
				gCtx,
				clientset,
				r,
				zulipClient,
				*namespace,
				health,
				metrics,
			); err != nil {
				return fmt.Errorf("watcher for rule '%s' failed: %w", r.Name, err)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		cancel()
		log.Fatalf("Application failed: %v", err)
	}
	cancel()
}
