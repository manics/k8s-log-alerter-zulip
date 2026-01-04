package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Config represents the application configuration
type Config struct {
	Zulip ZulipConfig     `json:"zulip"`
	Rules map[string]Rule `json:"rules"`
}

type ZulipConfig struct {
	Site             string `json:"site"`
	BotEmail         string `json:"bot_email"`
	BotKey           string `json:"bot_key"`
	Channel          string `json:"channel"`
	MaxMessageLength int    `json:"-"`
}

type Rule struct {
	Name      string            `json:"-"`
	PodLabels map[string]string `json:"pod_labels"`
	Regex     string            `json:"regex"`
	Compiled  *regexp.Regexp    `json:"-"`
}

// Shared HTTP client to enable connection pooling
var httpClient = &http.Client{
	Timeout: 10 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    30 * time.Second,
		DisableCompression: true,
	},
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
		rule.Name = name
		re, err := regexp.Compile(rule.Regex)
		if err != nil {
			return nil, fmt.Errorf("invalid regex for rule %s: %w", name, err)
		}
		rule.Compiled = re
		cfg.Rules[name] = rule
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
		if data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
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

// HealthChecker tracks the health of the application
type HealthChecker struct {
	mu sync.Mutex
	// errors indicates whether a rule name has an error
	errors map[string]error
}

// Report marks or unmarks a rule name as having an error
func (h *HealthChecker) Report(rule string, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.errors == nil {
		h.errors = make(map[string]error)
	}
	if err != nil {
		h.errors[rule] = err
	} else {
		delete(h.errors, rule)
	}
}

func (h *HealthChecker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.errors) > 0 {
		w.WriteHeader(http.StatusInternalServerError)
		for r, e := range h.errors {
			if _, err := fmt.Fprintf(w, "Error in rule '%s': %v\n", r, e); err != nil {
				log.Printf("Failed to write response: %v", err)
			}
		}
		return
	}
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte("ok")); err != nil {
		log.Printf("Failed to write response: %v", err)
	}
}

// runWatcher creates a watcher for a log rule
func runWatcher(
	ctx context.Context,
	client kubernetes.Interface,
	rule *Rule,
	zulipCfg *ZulipConfig,
	namespace string,
	health *HealthChecker,
) {
	labelSelector := labels.Set(rule.PodLabels).String()
	log.Printf("Starting watcher for rule '%s' labels: %s", rule.Name, labelSelector)

	// Keep track of active streams for this rule
	activeStreams := make(map[string]context.CancelFunc)
	var mu sync.Mutex

	for {
		// If the context has been cancelled exit the loop to allow a graceful shutdown
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Watch for pods with the given labels in the specified namespace (or all if empty)
		watcher, err := client.CoreV1().Pods(namespace).Watch(ctx, metav1.ListOptions{
			LabelSelector: labelSelector,
		})
		if err != nil {
			health.Report(rule.Name, err)
			log.Printf("Error watching pods for rule %s: %v", rule.Name, err)
			time.Sleep(5 * time.Second)
			continue
		}
		health.Report(rule.Name, nil)

		ch := watcher.ResultChan()
		for event := range ch {
			pod, ok := event.Object.(*corev1.Pod)
			if !ok {
				continue
			}

			switch event.Type {
			case "ADDED", "MODIFIED":
				if pod.Status.Phase == corev1.PodRunning {
					mu.Lock()
					for _, container := range pod.Spec.Containers {
						key := string(pod.UID) + " " + container.Name
						if _, exists := activeStreams[key]; !exists {
							log.Printf("Watching logs for pod %s[%s]", pod.Name, container.Name)
							streamCtx, cancel := context.WithCancel(ctx)
							activeStreams[key] = cancel
							go streamLogs(
								streamCtx,
								client,
								pod,
								&container,
								rule,
								zulipCfg,
								func() {
									mu.Lock()
									delete(activeStreams, key)
									mu.Unlock()
								},
							)
						}
					}
					mu.Unlock()
				}
			case "DELETED":
				mu.Lock()
				for _, container := range pod.Spec.Containers {
					key := string(pod.UID) + " " + container.Name
					if cancel, exists := activeStreams[key]; exists {
						cancel()
						delete(activeStreams, key)
					}
				}
				mu.Unlock()
			}
		}
		// If channel closed, restart watch
		watcher.Stop()
	}
}

// streamLogs monitors the logs for a single rule/pod/container, and sends Zulip alerts on matches
func streamLogs(
	ctx context.Context,
	client kubernetes.Interface,
	pod *corev1.Pod,
	container *corev1.Container,
	rule *Rule,
	zulipCfg *ZulipConfig,
	cleanup func(),
) {
	defer cleanup()
	defer log.Printf("Stopped watching logs for pod %s[%s]", pod.Name, container.Name)

	// Has to be greater than 0
	var sinceSeconds int64 = 1

	// We loop here to retry the log stream if it fails (e.g. network error).
	// We cannot rely on the main watcher loop to restart us because:
	// 1. The Pod is still 'Running', so no new K8s event is generated.
	// 2. Even if the Watcher restarts, there is a race condition: if the Watcher processes the existing Pod
	//    before this goroutine finishes cleanup, it will see the stream as 'active' and not restart it.
	for {
		if ctx.Err() != nil {
			return
		}

		req := client.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{
			Container:    container.Name,
			Follow:       true,
			SinceSeconds: &sinceSeconds,
		})

		stream, err := req.Stream(ctx)
		if err != nil {
			log.Printf("Error streaming logs for pod %s[%s]: %v", pod.Name, container.Name, err)
			// Retry afer 10s, unless cancelled by the parent
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Second):
				continue
			}
		}

		scanner := bufio.NewScanner(stream)
		for scanner.Scan() {
			line := scanner.Text()
			if rule.Compiled.MatchString(line) {
				sendZulipAlert(zulipCfg, rule, pod, container, line)
			}
		}
		if err := stream.Close(); err != nil {
			log.Printf("Error closing stream for pod %s[%s]: %v", pod.Name, container.Name, err)
		}

		if err := scanner.Err(); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf(
				"Log stream ended with error for pod %s[%s] (retrying): %v",
				pod.Name,
				container.Name,
				err,
			)
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
				continue
			}
		} else {
			return
		}
	}
}

// formatZulipContent creates the Zulip message content
//
// Message longer than maxLength are truncated and … is appended. If a message
// appears to be JSON it is pretty-printed
func formatZulipContent(
	pod *corev1.Pod,
	container *corev1.Container,
	message string,
	maxLength int,
) string {
	prefix := fmt.Sprintf("**Pod**: %s[%s]@%s\n", pod.Name, container.Name, pod.Spec.NodeName)

	maxLogLength := maxLength - len(prefix)

	if len(message) > maxLogLength {
		message = message[:maxLogLength-2] + " …"
	} else {
		// Try pretty printing as JSON, but not if it ends up being too long
		var obj any
		if err := json.Unmarshal([]byte(message), &obj); err == nil {
			if indented, err := json.MarshalIndent(obj, "", "  "); err == nil {
				json_message := "```json\n" + string(indented) + "\n```"
				if len(json_message) <= maxLogLength {
					message = json_message
				}
			}
		}
	}
	// else message is unchanged

	return fmt.Sprintf("%s%s", prefix, message)
}

// getZulipDetails checks the Zulip credentials are valid and gets the maximum message length
func getZulipDetails(cfg *ZulipConfig) error {
	// https://zulip.com/api/register-queue
	data := url.Values{}
	data.Set("event_types", "[\"realm\"]")

	apiURL := strings.TrimRight(cfg.Site, "/") + "/api/v1/register"

	req, err := http.NewRequest("POST", apiURL, strings.NewReader(data.Encode()))
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	req.SetBasicAuth(cfg.BotEmail, cfg.BotKey)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("error checking Zulip auth: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("error checking Zulip auth (%s): %s", resp.Status, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading Zulip auth response: %v", err)
	}

	var response map[string]any
	if err := json.Unmarshal(body, &response); err != nil {
		return fmt.Errorf("error parsing Zulip auth response: %v", err)
	}

	if result, ok := response["result"]; !ok || result != "success" {
		return fmt.Errorf("failed to authenticate with Zulip: %v", response)
	}

	log.Printf("Successfully authenticated with Zulip")

	if maxLen, ok := response["max_message_length"].(float64); ok {
		cfg.MaxMessageLength = int(maxLen)
	} else {
		return fmt.Errorf("failed to get max_message_length from Zulip")
	}

	if cfg.MaxMessageLength < 256 {
		return fmt.Errorf("server max_message_length is too small: %d", cfg.MaxMessageLength)
	}

	log.Printf("Maximum message length: %d", cfg.MaxMessageLength)
	return nil
}

// sendZulipAlert sends a message to a Zulip channel with topic set to the rule name
func sendZulipAlert(
	cfg *ZulipConfig,
	rule *Rule,
	pod *corev1.Pod,
	container *corev1.Container,
	message string,
) {
	log.Printf("[%s] pod %s[%s] Message: %s", rule.Name, pod.Name, container.Name, message)
	content := formatZulipContent(pod, container, message, cfg.MaxMessageLength)

	data := url.Values{}
	data.Set("type", "stream")
	data.Set("to", cfg.Channel)
	data.Set("topic", rule.Name)
	data.Set("content", content)

	apiURL := strings.TrimRight(cfg.Site, "/") + "/api/v1/messages"

	req, err := http.NewRequest("POST", apiURL, strings.NewReader(data.Encode()))
	if err != nil {
		log.Printf("Error creating request: %v", err)
		return
	}

	req.SetBasicAuth(cfg.BotEmail, cfg.BotKey)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := httpClient.Do(req)
	if err != nil {
		log.Printf("Error sending alert to Zulip: %v", err)
		return
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		log.Printf("Error response from Zulip (%s): %s", resp.Status, string(body))
	}
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

	var configPath string
	flag.StringVar(&configPath, "c", "", "Path to configuration file")

	var namespace string
	flag.StringVar(&namespace, "n", "", "Namespace")

	var listenAddr string
	flag.StringVar(&listenAddr, "healthcheck", ":8081", "Address to listen on for health checks")

	flag.Parse()

	if configPath == "" {
		flag.Usage()
		os.Exit(1)
	}

	cfg, err := loadConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load config %s: %v", configPath, err)
	}

	{
		err := getZulipDetails(&cfg.Zulip)
		if err != nil {
			log.Fatalf("%v", err)
		}
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

	if namespace == "" {
		namespace = currentNamespace
	}

	log.Printf("Using namespace: %s", namespace)

	health := &HealthChecker{}

	// We've validated that we can connect to the K8s API so now we'll report on whether
	// any of the rule watchers have failed
	go func() {
		srv := &http.Server{
			Addr:         listenAddr,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 10 * time.Second,
		}
		http.HandleFunc("/healthz", health.ServeHTTP)
		log.Printf("Starting healthcheck server on %s", listenAddr)
		if err := srv.ListenAndServe(); err != nil {
			log.Fatalf("Healthcheck server failed: %v", err)
		}
	}()

	// Start watchers
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Received shutdown signal, stopping watchers...")
		cancel()
	}()

	for _, rule := range cfg.Rules {
		wg.Add(1)
		go func(r Rule) {
			defer wg.Done()
			runWatcher(ctx, clientset, &r, &cfg.Zulip, namespace, health)
		}(rule)
	}

	wg.Wait()
}
