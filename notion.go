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

// --- Notion Connector ---
// Manages databases and pages via Notion API.
// Focused on content calendars, task boards, and structured data.

const (
	notionBaseURL    = "https://api.notion.com/v1"
	notionAPIVersion = "2022-06-28"
)

type NotionConfig struct {
	APIKeyEnv string `yaml:"api_key_env"` // env var holding the integration token
	apiKey    string
}

type NotionConnector struct {
	config NotionConfig
}

// --- Data Types ---

type NotionDatabase struct {
	ID          string                       `json:"id"`
	Title       []NotionRichText             `json:"title"`
	Description []NotionRichText             `json:"description,omitempty"`
	Properties  map[string]NotionPropertyDef `json:"properties"`
	CreatedTime string                       `json:"created_time"`
	URL         string                       `json:"url"`
}

type NotionPropertyDef struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Type string `json:"type"`
}

type NotionPage struct {
	ID             string                        `json:"id"`
	CreatedTime    string                        `json:"created_time"`
	LastEditedTime string                        `json:"last_edited_time"`
	URL            string                        `json:"url"`
	Properties     map[string]NotionPropertyValue `json:"properties"`
}

type NotionPropertyValue struct {
	Type     string              `json:"type"`
	Title    []NotionRichText    `json:"title,omitempty"`
	RichText []NotionRichText    `json:"rich_text,omitempty"`
	Number   *float64            `json:"number,omitempty"`
	Select   *NotionSelectOption `json:"select,omitempty"`
	Status   *NotionSelectOption `json:"status,omitempty"`
	Date     *NotionDateValue    `json:"date,omitempty"`
	Checkbox *bool               `json:"checkbox,omitempty"`
	URL      *string             `json:"url,omitempty"`
	MultiSelect []NotionSelectOption `json:"multi_select,omitempty"`
}

type NotionRichText struct {
	PlainText string `json:"plain_text"`
	Href      string `json:"href,omitempty"`
}

type NotionSelectOption struct {
	Name  string `json:"name"`
	Color string `json:"color,omitempty"`
}

type NotionDateValue struct {
	Start string `json:"start"`
	End   string `json:"end,omitempty"`
}

// --- Constructor ---

func NewNotionConnector(cfg NotionConfig, apiKey string) (*NotionConnector, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("notion: API key is empty")
	}

	cfg.apiKey = apiKey
	c := &NotionConnector{config: cfg}

	log.Printf("[notion] initialized")
	return c, nil
}

// --- HTTP ---

func (c *NotionConnector) request(method, path string, body interface{}) ([]byte, error) {
	var reqBody io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("notion: failed to marshal request: %w", err)
		}
		reqBody = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, notionBaseURL+path, reqBody)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+c.config.apiKey)
	req.Header.Set("Notion-Version", notionAPIVersion)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("notion: request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("notion: failed to read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("notion: %s %s returned %d: %s", method, path, resp.StatusCode, string(respBody))
	}

	return respBody, nil
}

// --- Databases ---

// ListDatabases returns all databases the integration has access to.
func (c *NotionConnector) ListDatabases() ([]NotionDatabase, error) {
	body := map[string]interface{}{
		"filter": map[string]string{
			"value":    "database",
			"property": "object",
		},
		"page_size": 50,
	}

	data, err := c.request("POST", "/search", body)
	if err != nil {
		return nil, err
	}

	var result struct {
		Results []NotionDatabase `json:"results"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("notion: failed to parse databases: %w", err)
	}

	return result.Results, nil
}

// GetDatabase returns a single database with its schema.
func (c *NotionConnector) GetDatabase(databaseID string) (*NotionDatabase, error) {
	data, err := c.request("GET", "/databases/"+databaseID, nil)
	if err != nil {
		return nil, err
	}

	var db NotionDatabase
	if err := json.Unmarshal(data, &db); err != nil {
		return nil, fmt.Errorf("notion: failed to parse database: %w", err)
	}

	return &db, nil
}

// QueryDatabase queries a database with an optional filter.
func (c *NotionConnector) QueryDatabase(databaseID string, filter json.RawMessage, limit int) ([]NotionPage, error) {
	body := map[string]interface{}{
		"page_size": limit,
	}
	if filter != nil {
		var f interface{}
		if err := json.Unmarshal(filter, &f); err == nil {
			body["filter"] = f
		}
	}

	data, err := c.request("POST", "/databases/"+databaseID+"/query", body)
	if err != nil {
		return nil, err
	}

	var result struct {
		Results []NotionPage `json:"results"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("notion: failed to parse query results: %w", err)
	}

	return result.Results, nil
}

// --- Pages ---

// GetPage returns a single page by ID.
func (c *NotionConnector) GetPage(pageID string) (*NotionPage, error) {
	data, err := c.request("GET", "/pages/"+pageID, nil)
	if err != nil {
		return nil, err
	}

	var page NotionPage
	if err := json.Unmarshal(data, &page); err != nil {
		return nil, fmt.Errorf("notion: failed to parse page: %w", err)
	}

	return &page, nil
}

// CreatePage creates a new page in a database.
func (c *NotionConnector) CreatePage(databaseID string, properties map[string]interface{}) (*NotionPage, error) {
	body := map[string]interface{}{
		"parent": map[string]string{
			"database_id": databaseID,
		},
		"properties": properties,
	}

	data, err := c.request("POST", "/pages", body)
	if err != nil {
		return nil, err
	}

	var page NotionPage
	if err := json.Unmarshal(data, &page); err != nil {
		return nil, fmt.Errorf("notion: failed to parse created page: %w", err)
	}

	return &page, nil
}

// UpdatePage updates properties of an existing page.
func (c *NotionConnector) UpdatePage(pageID string, properties map[string]interface{}) error {
	body := map[string]interface{}{
		"properties": properties,
	}
	_, err := c.request("PATCH", "/pages/"+pageID, body)
	return err
}

// --- Formatting ---

// extractTitle extracts the plain text title from a NotionPage.
func extractTitle(props map[string]NotionPropertyValue) string {
	for _, v := range props {
		if v.Type == "title" && len(v.Title) > 0 {
			return v.Title[0].PlainText
		}
	}
	return "(untitled)"
}

// extractPropertySummary returns a compact summary of a property value.
func extractPropertySummary(v NotionPropertyValue) string {
	switch v.Type {
	case "title":
		if len(v.Title) > 0 {
			return v.Title[0].PlainText
		}
	case "rich_text":
		if len(v.RichText) > 0 {
			return v.RichText[0].PlainText
		}
	case "number":
		if v.Number != nil {
			return fmt.Sprintf("%.0f", *v.Number)
		}
	case "select":
		if v.Select != nil {
			return v.Select.Name
		}
	case "status":
		if v.Status != nil {
			return v.Status.Name
		}
	case "date":
		if v.Date != nil {
			return v.Date.Start
		}
	case "checkbox":
		if v.Checkbox != nil {
			if *v.Checkbox {
				return "yes"
			}
			return "no"
		}
	case "url":
		if v.URL != nil {
			return *v.URL
		}
	case "multi_select":
		names := make([]string, len(v.MultiSelect))
		for i, s := range v.MultiSelect {
			names[i] = s.Name
		}
		return strings.Join(names, ", ")
	}
	return ""
}

// FormatNotionPagesForPrompt formats Notion pages for LLM consumption.
func FormatNotionPagesForPrompt(pages []NotionPage) string {
	if len(pages) == 0 {
		return "No pages."
	}

	var sb strings.Builder
	for i, p := range pages {
		title := extractTitle(p.Properties)
		sb.WriteString(fmt.Sprintf("%d. %s (id:%s)\n", i+1, title, p.ID))

		for name, val := range p.Properties {
			if val.Type == "title" {
				continue // already shown
			}
			summary := extractPropertySummary(val)
			if summary != "" {
				sb.WriteString(fmt.Sprintf("   %s: %s\n", name, summary))
			}
		}
	}
	return sb.String()
}

// FormatNotionDatabasesForPrompt formats Notion databases for LLM consumption.
func FormatNotionDatabasesForPrompt(dbs []NotionDatabase) string {
	if len(dbs) == 0 {
		return "No databases."
	}

	var sb strings.Builder
	for i, db := range dbs {
		title := "(untitled)"
		if len(db.Title) > 0 {
			title = db.Title[0].PlainText
		}

		propNames := make([]string, 0, len(db.Properties))
		for name, prop := range db.Properties {
			propNames = append(propNames, fmt.Sprintf("%s(%s)", name, prop.Type))
		}

		sb.WriteString(fmt.Sprintf("%d. %s (id:%s)\n", i+1, title, db.ID))
		sb.WriteString(fmt.Sprintf("   properties: %s\n", strings.Join(propNames, ", ")))
	}
	return sb.String()
}
