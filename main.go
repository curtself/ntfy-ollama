
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"
)

type NtfyMessage struct {
	ID      string   `json:"id"`
	Event   string   `json:"event"`
	Topic   string   `json:"topic"`
	Message string   `json:"message"`
	Title   string   `json:"title"`
	Tags    []string `json:"tags"`
}

type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type OllamaRequest struct {
	Model    string        `json:"model"`
	Messages []ChatMessage `json:"messages"`
	Stream   bool          `json:"stream"`
}

type OllamaResponse struct {
	Message ChatMessage `json:"message"`
}

var (
	ntfyURL     = strings.TrimRight(os.Getenv("NTFY_URL"), "/")
	ntfyAuth    = os.Getenv("NTFY_AUTH")
	ollamaURL   = strings.TrimRight(os.Getenv("OLLAMA_URL"), "/")
	ollamaModel = os.Getenv("OLLAMA_MODEL")

	streamClient = &http.Client{}
	ollamaClient = &http.Client{Timeout: 4 * time.Minute}
	ntfyClient   = &http.Client{Timeout: 15 * time.Second}
)

func subscribeNtfy(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ntfyURL+"/chat/json", nil)
	if err != nil { return err }

	req.Header.Set("Authorization", ntfyAuth)

	resp, err := streamClient.Do(req)
	if err != nil { return err }
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK { return fmt.Errorf("ntfy returned HTTP %s", resp.Status) }

	log.Println("Connected to ntfy")

	decoder := json.NewDecoder(resp.Body)

	for {
		var msg NtfyMessage
		if err := decoder.Decode(&msg); err != nil { return err }

		if msg.Event != "message" { continue }
		if slices.Contains(msg.Tags, "no-ai") { continue }

		log.Printf("Received message %s: %s", msg.ID, msg.Message)

		if err := processMessage(msg); err != nil {
			log.Printf("Message processing failed: %v", err)
		}
	}
}

func askOllama(prompt string) (string, error) {
	requestBody := OllamaRequest{
		Model: ollamaModel,
		Messages: []ChatMessage{
			{Role: "user", Content: prompt},
		},
		Stream: false,
	}

	body, err := json.Marshal(requestBody)
	if err != nil { return "", err }

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ollamaURL+"/api/chat", bytes.NewReader(body))
	if err != nil { return "", err }

	req.Header.Set("Content-Type", "application/json")

	resp, err := ollamaClient.Do(req)
	if err != nil { return "", fmt.Errorf("Ollama request failed: %w", err) }
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		errorBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("Ollama returned HTTP %s: %s", resp.Status, strings.TrimSpace(string(errorBody)))
	}

	var result OllamaResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil { return "", err }

	return result.Message.Content, nil
}

func publishNtfy(message string) error {
	req, err := http.NewRequest(http.MethodPost, ntfyURL+"/chat", strings.NewReader(message))
	if err != nil { return err }

	req.Header.Set("Authorization", ntfyAuth)
	req.Header.Set("Title", "Ollama")
	req.Header.Set("Tags", "no-ai")
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("Markdown", "yes")

	resp, err := ntfyClient.Do(req)
	if err != nil { return err }
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("ntfy publish returned HTTP %s", resp.Status)
	}

	return nil
}

func processMessage(msg NtfyMessage) error {
	response, err := askOllama(msg.Message)
	if err != nil {
		log.Printf("Ollama unavailable or request failed: %v", err)
		return publishNtfy("Ollama is currently unavailable. Please try again later.")
	}

	if strings.TrimSpace(response) == "" {
		return publishNtfy("Ollama returned an empty response.")
	}

	log.Printf("Ollama response received (%d characters)", len(response))

	return publishNtfy(response)
}

func main() {
	if ntfyURL == "" || ntfyAuth == "" || ollamaURL == "" || ollamaModel == "" {
		log.Fatal("NTFY_URL, NTFY_AUTH, OLLAMA_URL and OLLAMA_MODEL must be configured")
	}

	ctx := context.Background()

	for {
		err := subscribeNtfy(ctx)
		if err != nil { log.Printf("Subscription disconnected: %v", err) }

		log.Println("Reconnecting in 5 seconds...")
		time.Sleep(5 * time.Second)
	}
}

