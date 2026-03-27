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

// --- Make.com Connector ---
// Manages scenarios, executions, and blueprints via Make.com API v2.

type MakeConfig struct {
	APIKeyEnv string `yaml:"api_key_env"` // env var holding the API token
	Region    string `yaml:"region"`      // eu1, eu2, us1, us2
	TeamID    int    `yaml:"team_id"`
	apiKey    string
}

type MakeConnector struct {
	config  MakeConfig
	baseURL string
}

// --- Data Types ---

type MakeScenario struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	IsEnabled   bool   `json:"isEnabled"`
	IsPaused    bool   `json:"isPaused"`
	Scheduling  *MakeScheduling `json:"scheduling,omitempty"`
	CreatedAt   string `json:"createdAt"`
	UpdatedAt   string `json:"updatedAt"`
	NextExec    string `json:"nextExec,omitempty"`
}

type MakeScheduling struct {
	Type     string `json:"type"`     // indefinitely, once, daily, etc.
	Interval int    `json:"interval"` // minutes between runs
}

type MakeExecution struct {
	ID         int    `json:"id"`
	ScenarioID int    `json:"scenarioId"`
	Status     string `json:"status"` // success, warning, error
	Duration   int    `json:"duration"` // milliseconds
	Operations int    `json:"operations"`
	StartedAt  string `json:"startedAt"`
	FinishedAt string `json:"finishedAt"`
}

type MakeBlueprint struct {
	Flow []MakeBlueprintModule `json:"flow"`
	Name string                `json:"name"`
}

type MakeBlueprintModule struct {
	ID     int    `json:"id"`
	Module string `json:"module"`
	Mapper map[string]interface{} `json:"mapper,omitempty"`
}

// --- Constructor ---

func NewMakeConnector(cfg MakeConfig, apiKey string) (*MakeConnector, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("make: API key is empty")
	}

	region := cfg.Region
	if region == "" {
		region = "eu1"
	}

	c := &MakeConnector{
		config:  cfg,
		baseURL: fmt.Sprintf("https://%s.make.com/api/v2", region),
	}
	c.config.apiKey = apiKey

	log.Printf("[make] initialized (region: %s, team: %d)", region, cfg.TeamID)
	return c, nil
}

// --- HTTP ---

func (c *MakeConnector) request(method, path string, body interface{}) ([]byte, error) {
	var reqBody io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("make: failed to marshal request: %w", err)
		}
		reqBody = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, c.baseURL+path, reqBody)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Token "+c.config.apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("make: request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("make: failed to read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("make: %s %s returned %d: %s", method, path, resp.StatusCode, string(respBody))
	}

	return respBody, nil
}

// --- Scenarios ---

// ListScenarios returns all scenarios for the configured team.
func (c *MakeConnector) ListScenarios() ([]MakeScenario, error) {
	path := fmt.Sprintf("/scenarios?teamId=%d", c.config.TeamID)

	data, err := c.request("GET", path, nil)
	if err != nil {
		return nil, err
	}

	var result struct {
		Scenarios []MakeScenario `json:"scenarios"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("make: failed to parse scenarios: %w", err)
	}

	return result.Scenarios, nil
}

// GetScenario returns a single scenario by ID.
func (c *MakeConnector) GetScenario(scenarioID int) (*MakeScenario, error) {
	data, err := c.request("GET", fmt.Sprintf("/scenarios/%d", scenarioID), nil)
	if err != nil {
		return nil, err
	}

	var result struct {
		Scenario MakeScenario `json:"scenario"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("make: failed to parse scenario: %w", err)
	}

	return &result.Scenario, nil
}

// CreateScenario creates a new scenario with a blueprint.
// If no blueprint is provided, a minimal empty blueprint is used.
func (c *MakeConnector) CreateScenario(name string, teamID int, blueprint json.RawMessage) (*MakeScenario, error) {
	if blueprint == nil || len(blueprint) == 0 {
		blueprint = json.RawMessage(`{"flow":[{"id":1,"module":"json:ParseJSON","version":1,"mapper":{"json":"{}"},"metadata":{"designer":{"x":0,"y":0},"expect":[{"name":"json","type":"text","label":"JSON string","required":true}]}}],"metadata":{"version":1},"name":"` + name + `"}`)
	}

	// Make.com API expects blueprint and scheduling as JSON strings, not objects
	body := map[string]interface{}{
		"name":       name,
		"teamId":     teamID,
		"blueprint":  string(blueprint),
		"scheduling": `{"type":"indefinitely","interval":900}`,
	}

	data, err := c.request("POST", "/scenarios", body)
	if err != nil {
		return nil, err
	}

	var result struct {
		Scenario MakeScenario `json:"scenario"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("make: failed to parse created scenario: %w", err)
	}

	return &result.Scenario, nil
}

// UpdateScenario updates a scenario's properties.
func (c *MakeConnector) UpdateScenario(scenarioID int, updates map[string]interface{}) error {
	_, err := c.request("PATCH", fmt.Sprintf("/scenarios/%d", scenarioID), updates)
	return err
}

// ActivateScenario enables a scenario.
func (c *MakeConnector) ActivateScenario(scenarioID int) error {
	_, err := c.request("PATCH", fmt.Sprintf("/scenarios/%d", scenarioID), map[string]interface{}{
		"isEnabled": true,
	})
	return err
}

// DeactivateScenario disables a scenario.
func (c *MakeConnector) DeactivateScenario(scenarioID int) error {
	_, err := c.request("PATCH", fmt.Sprintf("/scenarios/%d", scenarioID), map[string]interface{}{
		"isEnabled": false,
	})
	return err
}

// RunScenario triggers an immediate execution.
func (c *MakeConnector) RunScenario(scenarioID int) error {
	_, err := c.request("POST", fmt.Sprintf("/scenarios/%d/run", scenarioID), nil)
	return err
}

// --- Executions ---

// ListExecutions returns recent executions for a scenario.
func (c *MakeConnector) ListExecutions(scenarioID int, limit int) ([]MakeExecution, error) {
	path := fmt.Sprintf("/scenarios/%d/executions?limit=%d&sortBy=startedAt&sortDir=desc", scenarioID, limit)

	data, err := c.request("GET", path, nil)
	if err != nil {
		return nil, err
	}

	var result struct {
		Executions []MakeExecution `json:"items"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("make: failed to parse executions: %w", err)
	}

	return result.Executions, nil
}

// ListFailedExecutions returns recent failed executions.
func (c *MakeConnector) ListFailedExecutions(scenarioID int, limit int) ([]MakeExecution, error) {
	execs, err := c.ListExecutions(scenarioID, limit*3) // fetch extra to filter
	if err != nil {
		return nil, err
	}

	var failed []MakeExecution
	for _, e := range execs {
		if e.Status == "error" {
			failed = append(failed, e)
			if len(failed) >= limit {
				break
			}
		}
	}

	return failed, nil
}

// --- Blueprints ---

// GetBlueprint returns the blueprint (workflow definition) of a scenario.
func (c *MakeConnector) GetBlueprint(scenarioID int) (*MakeBlueprint, error) {
	data, err := c.request("GET", fmt.Sprintf("/scenarios/%d/blueprint", scenarioID), nil)
	if err != nil {
		return nil, err
	}

	var result struct {
		Response struct {
			Blueprint MakeBlueprint `json:"blueprint"`
		} `json:"response"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("make: failed to parse blueprint: %w", err)
	}

	return &result.Response.Blueprint, nil
}

// SetBlueprint updates the blueprint of a scenario.
func (c *MakeConnector) SetBlueprint(scenarioID int, blueprint json.RawMessage) error {
	body := map[string]interface{}{
		"blueprint": blueprint,
	}
	_, err := c.request("PUT", fmt.Sprintf("/scenarios/%d/blueprint", scenarioID), body)
	return err
}

// --- Formatting ---

// FormatScenariosForPrompt formats scenarios for LLM consumption.
func FormatScenariosForPrompt(scenarios []MakeScenario) string {
	if len(scenarios) == 0 {
		return "No scenarios."
	}

	var sb strings.Builder
	for i, s := range scenarios {
		status := "disabled"
		if s.IsEnabled {
			status = "active"
		}
		if s.IsPaused {
			status = "paused"
		}
		sb.WriteString(fmt.Sprintf("%d. [%s] %s (id:%d)", i+1, status, s.Name, s.ID))
		if s.Scheduling != nil && s.Scheduling.Interval > 0 {
			sb.WriteString(fmt.Sprintf(" every %dm", s.Scheduling.Interval))
		}
		if s.NextExec != "" {
			sb.WriteString(fmt.Sprintf(" next: %s", s.NextExec))
		}
		sb.WriteString("\n")
	}
	return sb.String()
}

// FormatExecutionsForPrompt formats executions for LLM consumption.
func FormatExecutionsForPrompt(execs []MakeExecution) string {
	if len(execs) == 0 {
		return "No executions."
	}

	var sb strings.Builder
	for i, e := range execs {
		sb.WriteString(fmt.Sprintf("%d. [%s] scenario:%d ops:%d duration:%dms started:%s",
			i+1, e.Status, e.ScenarioID, e.Operations, e.Duration, e.StartedAt))
		sb.WriteString("\n")
	}
	return sb.String()
}
