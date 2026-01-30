package internal

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

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
