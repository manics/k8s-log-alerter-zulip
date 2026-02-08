package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
)

type ZulipConfig struct {
	Site             string `json:"site"`
	BotEmail         string `json:"bot_email"`
	BotKey           string `json:"bot_key"`
	Channel          string `json:"channel"`
	MaxMessageLength int    `json:"-"`
}

// ZulipClient handles Zulip interactions
type ZulipClient struct {
	Config     *ZulipConfig
	HTTPClient *http.Client
}

func NewZulipClient(cfg *ZulipConfig) (*ZulipClient, error) {
	c := &ZulipClient{
		Config: cfg,
		HTTPClient: &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:       10,
				IdleConnTimeout:    30 * time.Second,
				DisableCompression: true,
			},
		},
	}

	if err := c.checkAuth(); err != nil {
		return nil, err
	}
	return c, nil
}

// FormatContent creates the Zulip message content
//
// Message longer than MaxMessageLength are truncated and … is appended. If a message
// appears to be JSON it is pretty-printed
func (c *ZulipClient) FormatContent(
	pod *corev1.Pod,
	container *corev1.Container,
	message string,
) string {
	prefix := fmt.Sprintf("**Pod**: %s[%s]@%s\n", pod.Name, container.Name, pod.Spec.NodeName)

	maxLogLength := c.Config.MaxMessageLength - len(prefix)

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

// checkAuth checks the Zulip credentials are valid and gets the maximum message length
func (c *ZulipClient) checkAuth() error {
	// https://zulip.com/api/register-queue
	data := url.Values{}
	data.Set("event_types", "[\"realm\"]")

	apiURL := strings.TrimRight(c.Config.Site, "/") + "/api/v1/register"

	req, err := http.NewRequest("POST", apiURL, strings.NewReader(data.Encode()))
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	req.SetBasicAuth(c.Config.BotEmail, c.Config.BotKey)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.HTTPClient.Do(req)
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
		c.Config.MaxMessageLength = int(maxLen)
	} else {
		return fmt.Errorf("failed to get max_message_length from Zulip")
	}

	if c.Config.MaxMessageLength < 256 {
		return fmt.Errorf("server max_message_length is too small: %d", c.Config.MaxMessageLength)
	}

	log.Printf("Maximum message length: %d", c.Config.MaxMessageLength)
	return nil
}

// SendAlert sends a message to a Zulip channel with topic set to the rule name
func (c *ZulipClient) SendAlert(
	ctx context.Context,
	topic string,
	pod *corev1.Pod,
	container *corev1.Container,
	message string,
) error {
	log.Printf("[%s] pod %s[%s] Message: %s", topic, pod.Name, container.Name, message)
	content := c.FormatContent(pod, container, message)

	data := url.Values{}
	data.Set("type", "stream")
	data.Set("to", c.Config.Channel)
	data.Set("topic", topic)
	data.Set("content", content)

	apiURL := strings.TrimRight(c.Config.Site, "/") + "/api/v1/messages"

	req, err := http.NewRequestWithContext(ctx, "POST", apiURL, strings.NewReader(data.Encode()))
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}

	req.SetBasicAuth(c.Config.BotEmail, c.Config.BotKey)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("error sending alert to Zulip: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode >= 400 {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("error reading Zulip status %d response: %w", resp.StatusCode, err)
		}
		return fmt.Errorf("error response from Zulip (%s): %s", resp.Status, string(body))
	}

	return nil
}
