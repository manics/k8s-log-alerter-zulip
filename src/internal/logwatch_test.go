package internal

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

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
type MockAlerter struct{}

func (m *MockAlerter) SendAlert(topic string, pod *corev1.Pod, container *corev1.Container, message string) {
	log.Printf("MockAlerter topic='%s' pod=%s container=%s message=%s", topic, pod.Name, container.Name, message)
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
		GroupVersion: "v1", APIResources: []metav1.APIResource{{Name: "pods", Namespaced: true, Kind: "Pod"}},
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
		Name:      "Test Rule",
		PodLabels: map[string]string{"app": "test"},
		Regex:     "\\[test\\]",
	}
	var err error
	rule.Compiled, err = regexp.Compile(rule.Regex)
	if err != nil {
		t.Fatal(err)
	}

	ctx := t.Context()

	// Run watcher in a goroutine
	go RunWatcher(ctx, client, rule, &MockAlerter{}, "", &HealthChecker{})

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

	_, err = client.CoreV1().Pods("ns").Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
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
	err = client.CoreV1().Pods("ns").Delete(ctx, "test-pod", metav1.DeleteOptions{})
	if err != nil {
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
