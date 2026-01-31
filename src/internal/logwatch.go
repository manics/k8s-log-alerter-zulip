package internal

import (
	"bufio"
	"context"
	"log"
	"regexp"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

type Rule struct {
	Name      string            `json:"-"`
	PodLabels map[string]string `json:"pod_labels"`
	Regex     string            `json:"regex"`
	Compiled  *regexp.Regexp    `json:"-"`
}

// Alerter sends alerts
type Alerter interface {
	SendAlert(topic string, pod *corev1.Pod, container *corev1.Container, message string)
}

// RunWatcher creates a watcher for a log rule
func RunWatcher(
	ctx context.Context,
	client kubernetes.Interface,
	rule *Rule,
	alerter Alerter,
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
								alerter,
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
	alerter Alerter,
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
				alerter.SendAlert(rule.Name, pod, container, line)
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
