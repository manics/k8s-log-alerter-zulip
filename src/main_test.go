package main

import (
	"bytes"
	"log"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestConfigLoad(t *testing.T) {
	cfg, err := loadConfig("../config.json")
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Verify Zulip config
	if cfg.Zulip.Site != "https://zulip.example.com" {
		t.Errorf("Incorrect Site: '%s'", cfg.Zulip.Site)
	}
	if cfg.Zulip.BotEmail != "bot@example.com" {
		t.Errorf("Incorrect BotEmail: '%s'", cfg.Zulip.BotEmail)
	}
	if cfg.Zulip.BotKey != "secret" {
		t.Errorf("Incorrect BotKey: '%s'", cfg.Zulip.BotKey)
	}
	if cfg.Zulip.Channel != "alerts" {
		t.Errorf("Incorrect Channel: '%s'", cfg.Zulip.Channel)
	}
	if cfg.Zulip.MaxMessageLength != 0 {
		t.Errorf("Expected MaxMessageLength to be uninitialised: %d", cfg.Zulip.MaxMessageLength)
	}

	// Verify Rules
	if len(cfg.Rules) != 1 {
		t.Fatalf("Expected 1 rule: %d", len(cfg.Rules))
	}

	r1, ok := cfg.Rules["Cryptnono killed"]
	if !ok {
		t.Fatal("Rule 'Cryptnono killed' not found")
	}
	if r1.Name != "Cryptnono killed" {
		t.Errorf("Incorrect rule name: '%s'", r1.Name)
	}
	if r1.PodLabels["app.kubernetes.io/name"] != "cryptnono" {
		t.Errorf("Incorrect pod labels: %v", r1.PodLabels)
	}
	if r1.Regex != "\"action\":\\s*\"killed\"" {
		t.Errorf("Incorrect regex: '%s'", r1.Regex)
	}
	if r1.Compiled == nil {
		t.Error("Failed to compile regex")
	}
}

func TestFormatZulipContent(t *testing.T) {
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
			got := formatZulipContent(pod, container, tt.message, 68)
			if got != tt.expected {
				t.Errorf("formatZulipContent() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestRunWatcher(t *testing.T) {
	// Test the watcher by capturing the logs that are output when a matching pod is started/stopped
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)

	// Setup fake client
	client := fake.NewClientset()

	// Setup rule
	rule := &Rule{
		Name:      "Test Rule",
		PodLabels: map[string]string{"app": "test"},
		Regex:     ".*",
	}
	var err error
	rule.Compiled, err = regexp.Compile(rule.Regex)
	if err != nil {
		t.Fatal(err)
	}

	zulipCfg := &ZulipConfig{
		MaxMessageLength: 10000,
	}

	ctx := t.Context()

	// Run watcher in a goroutine
	go runWatcher(ctx, client, rule, zulipCfg, "", &HealthChecker{})

	// Allow watcher to start
	time.Sleep(100 * time.Millisecond)

	// Create a pod that matches
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
			Labels:    map[string]string{"app": "test"},
			UID:       "uid-1",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "test-container"}},
			NodeName:   "node-1",
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}

	_, err = client.CoreV1().Pods("default").Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Wait for log message indicating the watcher picked up the pod
	expected := "Watching logs for pod test-pod[test-container]"
	success := false
	for range 20 {
		if strings.Contains(buf.String(), expected) {
			success = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !success {
		t.Errorf("Did not find expected log message '%s':\n%s", expected, buf.String())
	}

	// TODO: check that matching pod logs generate an alert (need to mock Zulip)

	// Stop the pod
	err = client.CoreV1().Pods("default").Delete(ctx, "test-pod", metav1.DeleteOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Wait for log message indicating the watcher stopped watching the pod
	expected = "Stopped watching logs for pod test-pod[test-container]"
	success = false
	for range 20 {
		if strings.Contains(buf.String(), expected) {
			success = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !success {
		t.Errorf(
			"Did not find expected log message '%s' after pod deletion:\n%s",
			expected,
			buf.String(),
		)
	}
}
