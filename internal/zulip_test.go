package internal

import (
	"net/http"
	"net/http/httptest"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type MockZulipServer struct {
	requests []*http.Request
}

// mockZulipServer creates a mock Zulip server for testing
func (s *MockZulipServer) server(t *testing.T) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.requests = append(s.requests, r)

		if r.URL.Path == "/api/v1/register" {
			if r.Method != "POST" {
				t.Fatalf("Unexpected register method: %s", r.Method)
			}
			if _, err := w.Write(
				[]byte(`{"result": "success", "max_message_length": 1357}`),
			); err != nil {
				t.Fatal(err)
			}
			return
		}

		if r.URL.Path != "/api/v1/messages" {
			t.Fatalf("Unexpected path: %s", r.URL.Path)
		}
		if r.Method != "POST" {
			t.Fatalf("Unexpected method: %s", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
			t.Fatalf("Unexpected Content-Type: %s", r.Header.Get("Content-Type"))
		}

		if err := r.ParseForm(); err != nil {
			t.Fatalf("Error parsing form: %v", err)
		}

		for _, field := range []string{"type", "to", "topic", "content"} {
			if r.Form.Get(field) == "" {
				t.Fatalf("Missing form field: %s", field)
			}
		}

		w.WriteHeader(http.StatusOK)
	}))
}

func TestFormatContent(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pod",
		},
		Spec: corev1.PodSpec{
			NodeName: "test-node",
		},
	}
	container := &corev1.Container{
		Name: "test-container",
	}

	tests := []struct {
		name     string
		message  string
		expected string
	}{
		{
			name:     "Plain text",
			message:  "Something happened",
			expected: "**Pod**: test-pod[test-container]@test-node\nSomething happened",
		},
		{
			name:     "JSON message",
			message:  `{"a":2}`,
			expected: "**Pod**: test-pod[test-container]@test-node\n```json\n{\n  \"a\": 2\n}\n```",
		},
		{
			name:     "Invalid JSON",
			message:  `{"key": "value"`,
			expected: "**Pod**: test-pod[test-container]@test-node\n{\"key\": \"value\"",
		},
		{
			name:     "Long message",
			message:  `abcdefghijklmnopqrstuvwxyz`,
			expected: "**Pod**: test-pod[test-container]@test-node\nabcdefghijklmnopqrstuv …",
		},
		{
			name:     "Pretty JSON too long",
			message:  `{"a":{"b":1}}`,
			expected: "**Pod**: test-pod[test-container]@test-node\n{\"a\":{\"b\":1}}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ZulipClient{
				Config: &ZulipConfig{
					MaxMessageLength: 68,
				},
			}
			got := c.FormatContent(pod, container, tt.message)
			if got != tt.expected {
				t.Errorf("FormatContent() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestZulipClient(t *testing.T) {
	mock := &MockZulipServer{}
	zulipServer := mock.server(t)
	defer zulipServer.Close()

	cfg := &ZulipConfig{
		Site:     zulipServer.URL,
		BotEmail: "bot@example.com",
		BotKey:   "secret",
		Channel:  "alerts",
	}

	c, err := NewZulipClient(cfg)
	if err != nil {
		t.Fatalf("Failed to create zulip client: %v", err)
	}
	if c.Config.MaxMessageLength != 1357 {
		t.Errorf("Expected MaxMessageLength to be 1357, got %d", c.Config.MaxMessageLength)
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pod",
		},
		Spec: corev1.PodSpec{
			NodeName: "test-node",
			Containers: []corev1.Container{
				{
					Name: "test-container",
				},
			},
		},
	}
	if err := c.SendAlert("topic", pod, &pod.Spec.Containers[0], "message"); err != nil {
		t.Errorf("Failed to send alert: %v", err)
	}
	if err := c.SendAlert("topic", pod, &pod.Spec.Containers[0], "{\"a\":1}"); err != nil {
		t.Errorf("Failed to send alert: %v", err)
	}

	// Check all mock server requests
	if len(mock.requests) != 3 {
		t.Fatalf("Expected 3 requests, got %d", len(mock.requests))
	}

	if mock.requests[0].URL.Path != "/api/v1/register" {
		t.Errorf("Expected register request, got %s", mock.requests[0].URL.Path)
	}

	if mock.requests[1].URL.Path != "/api/v1/messages" {
		t.Errorf("Expected messages request, got %s", mock.requests[1].URL.Path)
	}
	expected1 := "**Pod**: test-pod[test-container]@test-node\nmessage"
	if mock.requests[1].Form.Get("content") != expected1 {
		t.Errorf("Expected content '%s', got %s", expected1, mock.requests[1].FormValue("content"))
	}

	if mock.requests[2].URL.Path != "/api/v1/messages" {
		t.Errorf("Expected messages request, got %s", mock.requests[2].URL.Path)
	}
	expected2 := "**Pod**: test-pod[test-container]@test-node\n```json\n{\n  \"a\": 1\n}\n```"
	if mock.requests[2].Form.Get("content") != expected2 {
		t.Errorf("Expected content '%s', got %s", expected2, mock.requests[2].FormValue("content"))
	}
}
