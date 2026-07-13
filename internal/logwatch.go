package internal

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/tidwall/gjson"
	"golang.org/x/time/rate"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

// Rule describes a rule for matching logs and sending alerts
// If rate-limiting is enabled:
// - JsonGroupingFields is a list of JSON keys whose values are joined to form a string that is used to group logs for rate-limiting
// - LimitFirstN will always alert on the first N logs
// - LimitIntervalSeconds is the minimum number of seconds between each subsequent alert
// - LimitResetSeconds is approximately when the rate limiter will be reset
type Rule struct {
	Name                 string            `json:"-"`
	PodLabels            map[string]string `json:"pod_labels"`
	Regex                string            `json:"regex"`
	Compiled             *regexp.Regexp    `json:"-"`
	JsonGroupingFields   []string          `json:"json_grouping_fields"`
	LimitFirstN          int               `json:"limit_first_n"`
	LimitIntervalSeconds int               `json:"limit_interval_seconds"`
	LimitResetSeconds    int               `json:"limit_reset_seconds"`
}

// Init initialises and validates the rule
func (r *Rule) Init(name string) error {
	if len(r.Regex) == 0 {
		return fmt.Errorf("regex is required in rule %s", name)
	}

	re := regexp.MustCompile(r.Regex)

	if r.LimitFirstN > 0 || r.LimitIntervalSeconds > 0 {
		if len(r.JsonGroupingFields) == 0 {
			return fmt.Errorf(
				"json_grouping_fields is required in rule %s since rate limits are enabled",
				name,
			)
		}
		if r.LimitResetSeconds <= 0 {
			return fmt.Errorf(
				"limit_reset_seconds must be greater than 0 in rule %s since rate limits are enabled",
				name,
			)
		}
	}

	r.Name = name
	r.Compiled = re
	return nil
}

// Alerter sends alerts
type Alerter interface {
	SendAlert(
		ctx context.Context,
		topic string,
		pod *corev1.Pod,
		container *corev1.Container,
		message string,
	) error
}

// LogwatchMetrics holds the Prometheus metrics for the log watcher
type LogwatchMetrics struct {
	ContainersMonitored prometheus.Gauge
	LogsTotal           prometheus.Counter
	LogsAlertsTotal     prometheus.Counter
	LogsAlertsSent      prometheus.Counter
}

// NewLogwatchMetrics creates a LogwatchMetrics
func NewLogwatchMetrics(reg prometheus.Registerer, namespace string) *LogwatchMetrics {
	m := &LogwatchMetrics{
		ContainersMonitored: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "containers_monitored_count",
			Help:      "Number of containers being monitored",
		}),
		LogsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "logs_total",
			Help:      "Number of logs processed",
		}),
		LogsAlertsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "logs_alerts_total",
			Help:      "Number of logs that triggered an alert",
		}),
		LogsAlertsSent: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "logs_alerts_sent",
			Help:      "Number of alerts sent after ratelimiting",
		}),
	}
	return m
}

// RunWatcher creates a watcher for a log rule
func RunWatcher(
	ctx context.Context,
	client kubernetes.Interface,
	rule *Rule,
	alerter Alerter,
	namespace string,
	health *HealthChecker,
	metrics *LogwatchMetrics,
) error {
	labelSelector := labels.Set(rule.PodLabels).String()
	log.Printf("Starting watcher for rule '%s' labels: %s", rule.Name, labelSelector)

	// Keep track of active streams for this rule
	activeStreams := make(map[string]context.CancelFunc)
	var mu sync.Mutex

	rateLimiter, err := getRateLimiter(ctx, rule)
	if err != nil {
		return err
	}

	for {
		// If the context has been cancelled exit the loop to allow a graceful shutdown
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		// Watch for pods with the given labels in the specified namespace (or all if empty)
		watcher, err := client.CoreV1().Pods(namespace).Watch(ctx, metav1.ListOptions{
			LabelSelector: labelSelector,
		})
		if err != nil {
			health.Report(rule.Name, err)
			log.Printf("Error watching pods for rule %s: %v", rule.Name, err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(5 * time.Second):
			}
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
							/* #nosec G118 */
							streamCtx, cancel := context.WithCancel(ctx)
							activeStreams[key] = cancel
							metrics.ContainersMonitored.Inc()
							go streamLogs(
								streamCtx,
								client,
								pod,
								&container,
								rule,
								alerter,
								rateLimiter,
								metrics,
								func() {
									mu.Lock()
									defer mu.Unlock()
									delete(activeStreams, key)
									metrics.ContainersMonitored.Dec()
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

// getRateLimiter creates the rate limiter for this rule if enabled
func getRateLimiter(ctx context.Context, rule *Rule) (*TTLCache[*rate.Limiter], error) {
	if len(rule.JsonGroupingFields) > 0 {
		ttl := time.Duration(rule.LimitResetSeconds) * time.Second
		pruneInterval := ttl / 10
		rateLimiter, err := NewTTLCache(
			ctx,
			ttl,
			pruneInterval,
			func() *rate.Limiter {
				return rate.NewLimiter(
					rate.Limit(1)/rate.Limit(rule.LimitIntervalSeconds),
					rule.LimitFirstN,
				)
			},
		)
		return rateLimiter, err
	}
	return nil, nil
}

// streamLogs monitors the logs for a single rule/pod/container, and sends Zulip alerts on matches
func streamLogs(
	ctx context.Context,
	client kubernetes.Interface,
	pod *corev1.Pod,
	container *corev1.Container,
	rule *Rule,
	alerter Alerter,
	rateLimiter *TTLCache[*rate.Limiter],
	metrics *LogwatchMetrics,
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
		// Increase buffer to 1MB to handle large log lines
		buf := make([]byte, 0, 64*1024)
		scanner.Buffer(buf, 1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			metrics.LogsTotal.Inc()
			err := evaluateRule(ctx, rule, rateLimiter, alerter, line, pod, container, metrics)
			if err != nil {
				log.Printf(
					"Error evaluating rule '%s' for pod %s[%s]: %v",
					rule.Name,
					pod.Name,
					container.Name,
					err,
				)
			}
		}
		if err := stream.Close(); err != nil {
			log.Printf(
				"Error closing stream for rule '%s' pod %s[%s]: %v",
				rule.Name,
				pod.Name,
				container.Name,
				err,
			)
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

// evaluateRule checks whether a line matches the rule and sends an alert if required
func evaluateRule(
	ctx context.Context,
	rule *Rule,
	rateLimiter *TTLCache[*rate.Limiter],
	alerter Alerter,
	line string,
	pod *corev1.Pod,
	container *corev1.Container,
	metrics *LogwatchMetrics,
) error {
	var err error
	if rule.Compiled.MatchString(line) {
		key := ""
		if rateLimiter != nil {
			key = getGroupingKey(rule, line)
		}

		metrics.LogsAlertsTotal.Inc()

		if key == "" {
			err = alerter.SendAlert(ctx, rule.Name, pod, container, line)
			metrics.LogsAlertsSent.Inc()
		} else {
			if rateLimiter.Get(key).Allow() {
				err = alerter.SendAlert(ctx, rule.Name, pod, container, line)
				metrics.LogsAlertsSent.Inc()
			} else {
				log.Printf(
					"Rate limit for rule:'%s' group:'%s', not sending alert for line: %s",
					rule.Name,
					key,
					line)
			}
		}
	}
	return err
}

// getGroupingKey looks up JSON fields, and concatenates them using ASCII 0x1F
// (Unit Separator).
// Missing fields are set to ""
// If line is not valid JSON returns ""
// If all fields are empty or missing returns ""
func getGroupingKey(rule *Rule, line string) string {
	const delimiter = "\x1f"

	if !gjson.Valid(line) {
		// TODO: log warning
		return ""
	}
	var values []string
	for _, jsonKey := range rule.JsonGroupingFields {
		result := gjson.Get(line, jsonKey)
		values = append(values, result.String())
	}

	allEmpty := true
	for _, v := range values {
		if v != "" {
			allEmpty = false
			break
		}
	}

	if allEmpty {
		return ""
	}

	return strings.Join(values, delimiter)
}
