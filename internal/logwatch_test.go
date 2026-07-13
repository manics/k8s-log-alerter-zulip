package internal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/time/rate"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	clientv1 "k8s.io/client-go/kubernetes/typed/core/v1"
	fakecorev1 "k8s.io/client-go/kubernetes/typed/core/v1/fake"
	restclient "k8s.io/client-go/rest"
	fakerest "k8s.io/client-go/rest/fake"
)

// MockPods implements clientv1.PodInterface and overrides GetLogs
type MockPods struct {
	clientv1.PodInterface
	podLogs map[string][]string
	t       *testing.T
}

// GetLogs overrides the fake pod logs
// https://github.com/kubernetes/client-go/blob/v0.35.0/kubernetes/typed/core/v1/fake/fake_pod_expansion.go#L67-L79
func (m *MockPods) GetLogs(name string, opts *corev1.PodLogOptions) *restclient.Request {
	logs, ok := m.podLogs[name]
	if !ok {
		m.t.Fatalf("No logs")
	}

	fakeClient := &fakerest.RESTClient{
		Client: fakerest.CreateHTTPClient(func(request *http.Request) (*http.Response, error) {
			resp := &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(strings.Join(logs, "\n"))),
			}
			return resp, nil
		}),
	}
	return fakeClient.Request()
}

// MockCoreV1 is a mock implementation of clientv1.CoreV1Interface
type MockCoreV1 struct {
	fakecorev1.FakeCoreV1
	podLogs map[string][]string
}

// Pods overrides the default Pods method to return MockPods
func (m *MockCoreV1) Pods(namespace string) clientv1.PodInterface {
	return &MockPods{
		PodInterface: m.FakeCoreV1.Pods(namespace),
		podLogs:      m.podLogs,
	}
}

// MockClientset is a mock k8s client
type MockClientset struct {
	*fake.Clientset
	mockCoreV1 *MockCoreV1
}

func (c *MockClientset) CoreV1() clientv1.CoreV1Interface {
	return c.mockCoreV1
}

// ThreadSafeBuffer is a wrapper around bytes.Buffer that is safe for concurrent use.
type ThreadSafeBuffer struct {
	b bytes.Buffer
	m sync.Mutex
}

func (b *ThreadSafeBuffer) Write(p []byte) (n int, err error) {
	b.m.Lock()
	defer b.m.Unlock()
	return b.b.Write(p)
}

func (b *ThreadSafeBuffer) String() string {
	b.m.Lock()
	defer b.m.Unlock()
	return b.b.String()
}

func waitForLog(buf *ThreadSafeBuffer, expected string) error {
	for range 20 {
		if strings.Contains(buf.String(), expected) {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("Did not find expected log message '%s':\n%s", expected, buf.String())
}

// MockAlerter implements Alerter for testing
type MockAlerter struct {
	messages []string
}

func (m *MockAlerter) SendAlert(
	ctx context.Context,
	topic string,
	pod *corev1.Pod,
	container *corev1.Container,
	message string,
) error {
	m.messages = append(m.messages, message)
	log.Printf(
		"MockAlerter topic='%s' pod=%s container=%s message=%s",
		topic,
		pod.Name,
		container.Name,
		message,
	)
	return nil
}

func TestRunWatcher(t *testing.T) {
	// Test the watcher by capturing the logs that are output when a matching pod is started/stopped, or a mesage matches the rule
	buf := ThreadSafeBuffer{}
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)

	const TEST_LOG_MESSAGE = "[test] log message"

	// Setup fake client
	baseClient := fake.NewClientset()
	mockLogs := map[string][]string{
		"test-pod": {"[fail] This message shouldn't match", TEST_LOG_MESSAGE},
	}

	baseClient.Resources = append(baseClient.Resources, &metav1.APIResourceList{
		GroupVersion: "v1",
		APIResources: []metav1.APIResource{{Name: "pods", Namespaced: true, Kind: "Pod"}},
	})

	coreClient := &MockCoreV1{
		FakeCoreV1: *baseClient.CoreV1().(*fakecorev1.FakeCoreV1),
		podLogs:    mockLogs,
	}
	client := &MockClientset{
		Clientset:  baseClient,
		mockCoreV1: coreClient,
	}

	// Setup rule
	rule := &Rule{
		PodLabels: map[string]string{"app": "test"},
		Regex:     "\\[test\\]",
	}
	if err := rule.Init("Test Rule"); err != nil {
		t.Fatal(err)
	}

	ctx := t.Context()

	// Run watcher in a goroutine
	go func() {
		metrics := NewLogwatchMetrics(prometheus.NewRegistry(), "test")
		if err := RunWatcher(
			ctx,
			client,
			rule,
			&MockAlerter{},
			"",
			&HealthChecker{},
			metrics,
		); err != nil {
			t.Errorf("RunWatcher failed: %v", err)
		}
	}()

	// Allow watcher to start
	time.Sleep(100 * time.Millisecond)

	// Create a pod that matches
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "ns",
			Labels:    map[string]string{"app": "test"},
			UID:       "uid-1",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "test-container"}},
			NodeName:   "node-1",
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}

	if _, err := client.CoreV1().Pods("ns").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	// Wait for log message indicating the watcher picked up the pod
	if err := waitForLog(&buf, "Watching logs for pod test-pod[test-container]"); err != nil {
		t.Fatal(err)
	}

	// Useful for debugging failures
	t.Log(buf.String())

	// Wait for log indicating the rule was matched
	if err := waitForLog(
		&buf,
		"MockAlerter topic='Test Rule' pod=test-pod container=test-container message="+TEST_LOG_MESSAGE,
	); err != nil {
		t.Fatal(err)
	}

	// Stop the pod
	if err := client.CoreV1().
		Pods("ns").
		Delete(ctx, "test-pod", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}

	// Wait for log message indicating the watcher stopped watching the pod
	if err := waitForLog(
		&buf,
		"Stopped watching logs for pod test-pod[test-container]",
	); err != nil {
		t.Fatal(err)
	}

	// Now check the full set of logs
	if strings.Contains(buf.String(), "[fail]") {
		t.Fatal("Error: '[fail]' should not have been matched")
	}

	expected_logs := []string{
		"Starting watcher for rule 'Test Rule' labels: app=test",
		"Watching logs for pod test-pod[test-container]",
		"MockAlerter topic='Test Rule' pod=test-pod container=test-container message=" + TEST_LOG_MESSAGE,
		"Stopped watching logs for pod test-pod[test-container]",
	}

	logs := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(expected_logs) != len(logs) {
		t.Fatalf("Expected %d logs, got %d", len(expected_logs), len(logs))
	}

	for idx, log := range logs {
		// Skip date/time
		log = log[20:]
		if log != expected_logs[idx] {
			t.Errorf("Expected log '%s', got '%s'", expected_logs[idx], log)
		}
	}
}

func TestEvaluateRule(t *testing.T) {
	alerter := &MockAlerter{}
	rateLimiter, err := NewTTLCache(
		t.Context(),
		2*time.Second,
		200*time.Millisecond,
		func() *rate.Limiter {
			return rate.NewLimiter(
				rate.Limit(0.5),
				// bucket (burst):
				2,
			)
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	rule := &Rule{
		PodLabels:          map[string]string{"app": "test"},
		Regex:              "\\[test\\]",
		JsonGroupingFields: []string{"group"},
	}
	if err := rule.Init("Test Rule"); err != nil {
		t.Fatal(err)
	}

	// Create a pod that matches
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "ns",
			Labels:    map[string]string{"app": "test"},
			UID:       "uid-1",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "test-container"}},
			NodeName:   "node-1",
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}

	metrics := NewLogwatchMetrics(prometheus.NewRegistry(), "test")

	type testData struct {
		group string
		msg   string
	}

	testItems := []testData{
		{"g1", "[no match]"},
		// Empty group
		{"", "a [test]"},
		{"", "b [test]"},
		{"", "c [test]"},
		// Group g1
		{"g1", "g1-a [test]"},
		{"g1", "g1-b [test]"},
		{"g1", "g1-c [test]"},
		// Group g2
		{"g2", "g2-a [test]"},
	}

	for i := range 3 {
		time.Sleep(500 * time.Millisecond)
		for _, item := range testItems {
			line := fmt.Sprintf(`{"group": "%s", "msg": "%s %d"}`, item.group, item.msg, i)
			err := evaluateRule(
				t.Context(),
				rule,
				rateLimiter,
				alerter,
				line,
				pod,
				&pod.Spec.Containers[0],
				metrics,
			)
			if err != nil {
				t.Errorf("%v", err)
			}
		}
		time.Sleep(600 * time.Millisecond)
	}

	// This should be enough to reset the limiter, so burst=2 should take effect again
	time.Sleep(2 * time.Second)
	for _, item := range testItems {
		line := fmt.Sprintf(`{"group": "%s", "msg": "%s 😀"}`, item.group, item.msg)
		err := evaluateRule(
			t.Context(),
			rule,
			rateLimiter,
			alerter,
			line,
			pod,
			&pod.Spec.Containers[0],
			metrics,
		)
		if err != nil {
			t.Errorf("%v", err)
		}
	}

	// The 'group' field provides the group identifiers used for rate limiting
	//
	// A match with empty group is never rate limited
	//
	// The rate limit is 1/s, but with Burst=2 so "g1-a [test]" and "g1-b [test]"
	// should appear the first time
	//
	// Due to the burst it takes 2s before another alert is allowed for g1
	// so on the next round all g1 alerts are supressed
	//
	// Subsequently the limit of 1/s applies so only "g1-a [test]" for g1 is allowed
	//
	// "g2-a [test]" is a different group g2 so is always within the 1/s rate limit
	expected_messages := []string{
		// "[no match]", never matches

		`{"group": "", "msg": "a [test] 0"}`,
		`{"group": "", "msg": "b [test] 0"}`,
		`{"group": "", "msg": "c [test] 0"}`,

		`{"group": "g1", "msg": "g1-a [test] 0"}`,
		`{"group": "g1", "msg": "g1-b [test] 0"}`,
		// "g1-c [test] 0", rate limited
		`{"group": "g2", "msg": "g2-a [test] 0"}`,

		`{"group": "", "msg": "a [test] 1"}`,
		`{"group": "", "msg": "b [test] 1"}`,
		`{"group": "", "msg": "c [test] 1"}`,

		// "g1-a [test] 1", rate limited
		// "g1-b [test] 1", rate limited
		// "g1-c [test] 1", rate limited
		`{"group": "g2", "msg": "g2-a [test] 1"}`,

		`{"group": "", "msg": "a [test] 2"}`,
		`{"group": "", "msg": "b [test] 2"}`,
		`{"group": "", "msg": "c [test] 2"}`,

		`{"group": "g1", "msg": "g1-a [test] 2"}`,
		// "g1-b [test] 2", rate limited
		// "g1-c [test] 2", rate limited
		`{"group": "g2", "msg": "g2-a [test] 2"}`,

		// 2s delay means the limits are reset

		`{"group": "", "msg": "a [test] 😀"}`,
		`{"group": "", "msg": "b [test] 😀"}`,
		`{"group": "", "msg": "c [test] 😀"}`,

		`{"group": "g1", "msg": "g1-a [test] 😀"}`,
		`{"group": "g1", "msg": "g1-b [test] 😀"}`,
		// "g1-c [test] 😀", rate limited
		`{"group": "g2", "msg": "g2-a [test] 😀"}`,
	}
	if len(expected_messages) != len(alerter.messages) {
		t.Fatalf(
			"Expected %d alerts, got %d: %v",
			len(expected_messages),
			len(alerter.messages),
			alerter.messages,
		)
	}
	for idx, message := range alerter.messages {
		if message != expected_messages[idx] {
			t.Errorf("[%d] Expected alert '%s', got '%s'", idx, expected_messages[idx], message)
		}
	}
}

func TestGetGroupingKey(t *testing.T) {
	tests := []struct {
		name     string
		fields   []string
		line     string
		expected string
	}{
		{
			name:     "Single field exists",
			fields:   []string{"a"},
			line:     `{"a": "val"}`,
			expected: "val",
		},
		{
			name:     "Multiple fields exist",
			fields:   []string{"a", "b"},
			line:     `{"a": "val1", "b": "val2"}`,
			expected: "val1\x1fval2",
		},
		{
			name:     "Some fields missing",
			fields:   []string{"a", "c"},
			line:     `{"a": "val1", "b": "val2"}`,
			expected: "val1\x1f",
		},
		{
			name:     "Nested fields",
			fields:   []string{"a.b"},
			line:     `{"a": {"b": "val"}}`,
			expected: "val",
		},
		{
			name:     "All fields missing",
			fields:   []string{"c"},
			line:     `{"a": "val"}`,
			expected: "",
		},
		{
			name:     "Invalid JSON",
			fields:   []string{"a"},
			line:     `not json`,
			expected: "",
		},
		{
			name:     "Empty values",
			fields:   []string{"a", "b"},
			line:     `{"a": "", "b": ""}`,
			expected: "",
		},
		{
			name:     "Mixed empty and non-empty",
			fields:   []string{"a", "b"},
			line:     `{"a": "val", "b": ""}`,
			expected: "val\x1f",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rule := &Rule{
				JsonGroupingFields: tt.fields,
			}
			got := getGroupingKey(rule, tt.line)
			if got != tt.expected {
				t.Errorf("getGroupingKey() = %q, expected %q", got, tt.expected)
			}
		})
	}
}
