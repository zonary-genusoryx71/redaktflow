package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

// --- n8n Connector ---
// Manages workflows, executions, and credentials via n8n REST API v1.
// Works with both self-hosted and n8n Cloud instances.

type N8NConfig struct {
	BaseURL   string `yaml:"base_url"`   // e.g. https://your-n8n.example.com
	APIKeyEnv string `yaml:"api_key_env"` // env var holding the API key
	apiKey    string
}

type N8NConnector struct {
	config N8NConfig
}

// --- Data Types ---

type N8NWorkflow struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Active    bool            `json:"active"`
	Nodes     []N8NNode       `json:"nodes"`
	CreatedAt string          `json:"createdAt"`
	UpdatedAt string          `json:"updatedAt"`
	Tags      []N8NTag        `json:"tags,omitempty"`
}

type N8NNode struct {
	ID         string                 `json:"id"`
	Name       string                 `json:"name"`
	Type       string                 `json:"type"`
	Position   [2]int                 `json:"position"`
	Parameters map[string]interface{} `json:"parameters,omitempty"`
}

type N8NTag struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type N8NExecution struct {
	ID         string `json:"id"`
	WorkflowID string `json:"workflowId"`
	Status     string `json:"status"` // success, error, waiting, running
	StartedAt  string `json:"startedAt"`
	StoppedAt  string `json:"stoppedAt"`
	Mode       string `json:"mode"` // trigger, manual, webhook, etc.
}

type N8NCredential struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Type string `json:"type"`
}

// --- Constructor ---

func NewN8NConnector(cfg N8NConfig, apiKey string) (*N8NConnector, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("n8n: API key is empty")
	}
	if cfg.BaseURL == "" {
		return nil, fmt.Errorf("n8n: base_url is required")
	}

	cfg.apiKey = apiKey
	c := &N8NConnector{config: cfg}

	log.Printf("[n8n] initialized (base: %s)", cfg.BaseURL)
	return c, nil
}

// --- HTTP ---

func (c *N8NConnector) request(method, path string, body interface{}) ([]byte, error) {
	var reqBody io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("n8n: failed to marshal request: %w", err)
		}
		reqBody = bytes.NewReader(data)
	}

	url := strings.TrimRight(c.config.BaseURL, "/") + "/api/v1" + path
	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-N8N-API-KEY", c.config.apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("n8n: request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("n8n: failed to read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("n8n: %s %s returned %d: %s", method, path, resp.StatusCode, string(respBody))
	}

	return respBody, nil
}

// --- Workflows ---

// ListWorkflows returns all workflows.
func (c *N8NConnector) ListWorkflows() ([]N8NWorkflow, error) {
	data, err := c.request("GET", "/workflows", nil)
	if err != nil {
		return nil, err
	}

	var result struct {
		Data []N8NWorkflow `json:"data"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("n8n: failed to parse workflows: %w", err)
	}

	return result.Data, nil
}

// GetWorkflow returns a single workflow by ID with full node details.
func (c *N8NConnector) GetWorkflow(workflowID string) (*N8NWorkflow, error) {
	data, err := c.request("GET", "/workflows/"+workflowID, nil)
	if err != nil {
		return nil, err
	}

	var wf N8NWorkflow
	if err := json.Unmarshal(data, &wf); err != nil {
		return nil, fmt.Errorf("n8n: failed to parse workflow: %w", err)
	}

	return &wf, nil
}

// CreateWorkflow creates a new workflow.
func (c *N8NConnector) CreateWorkflow(name string, nodes []N8NNode) (*N8NWorkflow, error) {
	body := map[string]interface{}{
		"name":  name,
		"nodes": nodes,
	}

	data, err := c.request("POST", "/workflows", body)
	if err != nil {
		return nil, err
	}

	var wf N8NWorkflow
	if err := json.Unmarshal(data, &wf); err != nil {
		return nil, fmt.Errorf("n8n: failed to parse created workflow: %w", err)
	}

	return &wf, nil
}

// UpdateWorkflow updates a workflow's definition.
func (c *N8NConnector) UpdateWorkflow(workflowID string, updates map[string]interface{}) error {
	_, err := c.request("PATCH", "/workflows/"+workflowID, updates)
	return err
}

// ActivateWorkflow enables a workflow.
func (c *N8NConnector) ActivateWorkflow(workflowID string) error {
	return c.UpdateWorkflow(workflowID, map[string]interface{}{"active": true})
}

// DeactivateWorkflow disables a workflow.
func (c *N8NConnector) DeactivateWorkflow(workflowID string) error {
	return c.UpdateWorkflow(workflowID, map[string]interface{}{"active": false})
}

// DeleteWorkflow removes a workflow.
func (c *N8NConnector) DeleteWorkflow(workflowID string) error {
	_, err := c.request("DELETE", "/workflows/"+workflowID, nil)
	return err
}

// --- Executions ---

// ListExecutions returns recent executions, optionally filtered by workflow.
func (c *N8NConnector) ListExecutions(workflowID string, limit int) ([]N8NExecution, error) {
	path := fmt.Sprintf("/executions?limit=%d", limit)
	if workflowID != "" {
		path += "&workflowId=" + workflowID
	}

	data, err := c.request("GET", path, nil)
	if err != nil {
		return nil, err
	}

	var result struct {
		Data []N8NExecution `json:"data"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("n8n: failed to parse executions: %w", err)
	}

	return result.Data, nil
}

// ListFailedExecutions returns recent failed executions.
func (c *N8NConnector) ListFailedExecutions(workflowID string, limit int) ([]N8NExecution, error) {
	path := fmt.Sprintf("/executions?limit=%d&status=error", limit)
	if workflowID != "" {
		path += "&workflowId=" + workflowID
	}

	data, err := c.request("GET", path, nil)
	if err != nil {
		return nil, err
	}

	var result struct {
		Data []N8NExecution `json:"data"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("n8n: failed to parse executions: %w", err)
	}

	return result.Data, nil
}

// RetryExecution retries a failed execution.
func (c *N8NConnector) RetryExecution(executionID string) error {
	_, err := c.request("POST", fmt.Sprintf("/executions/%s/retry", executionID), nil)
	return err
}

// --- Credentials ---

// ListCredentials returns all credentials (metadata only, no secrets).
func (c *N8NConnector) ListCredentials() ([]N8NCredential, error) {
	data, err := c.request("GET", "/credentials", nil)
	if err != nil {
		return nil, err
	}

	var result struct {
		Data []N8NCredential `json:"data"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("n8n: failed to parse credentials: %w", err)
	}

	return result.Data, nil
}

// --- Formatting ---

// FormatWorkflowsForPrompt formats workflows for LLM consumption.
func FormatWorkflowsForPrompt(workflows []N8NWorkflow) string {
	if len(workflows) == 0 {
		return "No workflows."
	}

	var sb strings.Builder
	for i, w := range workflows {
		status := "inactive"
		if w.Active {
			status = "active"
		}
		sb.WriteString(fmt.Sprintf("%d. [%s] %s (id:%s, nodes:%d)",
			i+1, status, w.Name, w.ID, len(w.Nodes)))
		if len(w.Tags) > 0 {
			tags := make([]string, len(w.Tags))
			for j, t := range w.Tags {
				tags[j] = t.Name
			}
			sb.WriteString(fmt.Sprintf(" tags: %s", strings.Join(tags, ", ")))
		}
		sb.WriteString("\n")
	}
	return sb.String()
}

// FormatN8NExecutionsForPrompt formats n8n executions for LLM consumption.
func FormatN8NExecutionsForPrompt(execs []N8NExecution) string {
	if len(execs) == 0 {
		return "No executions."
	}

	var sb strings.Builder
	for i, e := range execs {
		sb.WriteString(fmt.Sprintf("%d. [%s] workflow:%s mode:%s started:%s",
			i+1, e.Status, e.WorkflowID, e.Mode, e.StartedAt))
		if e.StoppedAt != "" {
			sb.WriteString(fmt.Sprintf(" stopped:%s", e.StoppedAt))
		}
		sb.WriteString("\n")
	}
	return sb.String()
}
