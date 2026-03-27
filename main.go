package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"
)

// --- Config ---

type Config struct {
	Telegram  TelegramConfig         `yaml:"telegram"`
	Make      MakeConfig             `yaml:"make"`
	N8N       N8NConfig              `yaml:"n8n"`
	Notion    NotionConfig           `yaml:"notion"`
	Provider  ProviderConfig         `yaml:"provider"`
	Models    map[string]ModelConfig `yaml:"models"`
	Roles     map[string]string      `yaml:"roles"`
	Budgets   BudgetConfig           `yaml:"budgets"`
	Timeouts  TimeoutConfig          `yaml:"timeouts"`
	Pipelines []PipelineConfig       `yaml:"pipelines"`
}

type TelegramConfig struct {
	TokenEnv string          `yaml:"token_env"`
	ChatID   int64           `yaml:"chat_id"`
	Security ChannelSecurity `yaml:"security"`
	token    string
}

type ChannelSecurity struct {
	AllowedUsers   []int64 `yaml:"allowed_users"`
	MaxInputLength int     `yaml:"max_input_length"`
	RateLimit      int     `yaml:"rate_limit"`
	StripMarkdown  bool    `yaml:"strip_markdown"`
}

type ProviderConfig struct {
	Type      string `yaml:"type"`
	APIKeyEnv string `yaml:"api_key_env"`
	BaseURL   string `yaml:"base_url"`
	apiKey    string
}

type ModelConfig struct {
	Model     string  `yaml:"model"`
	MaxTokens int     `yaml:"max_tokens"`
	CostIn    float64 `yaml:"cost_per_1k_input"`
	CostOut   float64 `yaml:"cost_per_1k_output"`
}

type BudgetConfig struct {
	PerStepTokens     int `yaml:"per_step_tokens"`
	PerPipelineTokens int `yaml:"per_pipeline_tokens"`
	PerDayTokens      int `yaml:"per_day_tokens"`
}

type TimeoutConfig struct {
	AICall           string `yaml:"ai_call"`
	OperatorApproval string `yaml:"operator_approval"`
	PipelineTotal    string `yaml:"pipeline_total"`
}

type PipelineConfig struct {
	Name     string       `yaml:"name"`
	Schedule string       `yaml:"schedule"`
	Steps    []StepConfig `yaml:"steps"`
}

type StepConfig struct {
	Name         string            `yaml:"name"`
	Type         string            `yaml:"type"` // deterministic | ai | approval
	Action       string            `yaml:"action,omitempty"`
	Skill        string            `yaml:"skill,omitempty"`
	Prompt       string            `yaml:"prompt,omitempty"`
	Role         string            `yaml:"role,omitempty"`
	Vars         map[string]string `yaml:"vars,omitempty"`
	OutputSchema map[string]interface{} `yaml:"output_schema,omitempty"`
	Mode         string            `yaml:"mode,omitempty"`
	Channel      string            `yaml:"channel,omitempty"`
}

// --- Skill Registry ---

type Skill struct {
	Name         string                 `yaml:"name"`
	Description  string                 `yaml:"description"`
	Role         string                 `yaml:"role"`
	Prompt       string                 `yaml:"prompt"`
	OutputSchema map[string]interface{} `yaml:"output_schema"`
}

type SkillRegistry struct {
	skills map[string]Skill
}

func (r *SkillRegistry) Get(name string) (Skill, bool) {
	s, ok := r.skills[name]
	return s, ok
}

func loadSkills(dir string) (*SkillRegistry, error) {
	reg := &SkillRegistry{skills: make(map[string]Skill)}

	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return reg, nil
		}
		return nil, err
	}

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".yaml") {
			continue
		}

		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			log.Printf("[skills] failed to read %s: %v", e.Name(), err)
			continue
		}

		var skill Skill
		if err := yaml.Unmarshal(data, &skill); err != nil {
			log.Printf("[skills] failed to parse %s: %v", e.Name(), err)
			continue
		}

		reg.skills[skill.Name] = skill
		log.Printf("[skills] loaded: %s (%s)", skill.Name, skill.Description)
	}

	return reg, nil
}

// --- Budget ---

type Budget struct {
	mu       sync.Mutex
	dayTotal int
	dayStart time.Time
}

func (b *Budget) check(dayLimit, stepLimit int) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	if now.YearDay() != b.dayStart.YearDay() || now.Year() != b.dayStart.Year() {
		b.dayTotal = 0
		b.dayStart = now
	}

	if b.dayTotal+stepLimit > dayLimit {
		return fmt.Errorf("budget: daily limit exceeded (%d/%d)", b.dayTotal, dayLimit)
	}

	return nil
}

func (b *Budget) record(tokens int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.dayTotal += tokens
}

// --- Operator Channel ---

type OperatorChannel interface {
	Send(msg string) error
	WaitForApproval(ctx context.Context, msg string) (string, error)
}

type TelegramChannel struct {
	token  string
	chatID int64
}

func (t *TelegramChannel) Send(msg string) error {
	if len(msg) > 4096 {
		msg = msg[:4093] + "..."
	}

	body := map[string]interface{}{
		"chat_id":    t.chatID,
		"text":       msg,
		"parse_mode": "Markdown",
	}

	data, _ := json.Marshal(body)
	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", t.token)
	resp, err := http.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("telegram send failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("telegram send returned %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

func (t *TelegramChannel) WaitForApproval(ctx context.Context, msg string) (string, error) {
	if err := t.Send(msg + "\n\nReply: approve / edit <text> / reject"); err != nil {
		return "", err
	}

	// Poll for response (simplified -- production would use webhook)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return "timeout", nil
		case <-ticker.C:
			// In production: check for reply via getUpdates
			// For now: auto-timeout handled by context
		}
	}
}

// --- Pipeline Runner ---

var (
	makeConn   *MakeConnector
	n8nConn    *N8NConnector
	notionConn *NotionConnector
)

func runPipeline(ctx context.Context, pipeline PipelineConfig, cfg Config, skills *SkillRegistry, ch OperatorChannel, budget *Budget) error {
	data := make(map[string]interface{})

	pipelineTimeout, _ := time.ParseDuration(cfg.Timeouts.PipelineTotal)
	if pipelineTimeout == 0 {
		pipelineTimeout = 5 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, pipelineTimeout)
	defer cancel()

	log.Printf("[pipeline:%s] starting", pipeline.Name)

	for _, step := range pipeline.Steps {
		select {
		case <-ctx.Done():
			return fmt.Errorf("pipeline timeout after %s", pipelineTimeout)
		default:
		}

		log.Printf("[pipeline:%s][step:%s] type=%s", pipeline.Name, step.Name, step.Type)

		switch step.Type {
		case "deterministic":
			log.Printf("[pipeline:%s][step:%s] action=%s", pipeline.Name, step.Name, step.Action)
			switch step.Action {

			// --- Make.com actions ---

			case "make_list_scenarios":
				if makeConn == nil {
					return fmt.Errorf("[step:%s] make connector not configured", step.Name)
				}
				scenarios, err := makeConn.ListScenarios()
				if err != nil {
					return fmt.Errorf("[step:%s] make list scenarios failed: %w", step.Name, err)
				}
				data["scenarios"] = FormatScenariosForPrompt(scenarios)
				data["scenario_count"] = fmt.Sprintf("%d", len(scenarios))
				log.Printf("[pipeline:%s][step:%s] fetched %d scenarios", pipeline.Name, step.Name, len(scenarios))

			case "make_failed_executions":
				if makeConn == nil {
					return fmt.Errorf("[step:%s] make connector not configured", step.Name)
				}
				scenarioIDStr, _ := step.Vars["scenario_id"]
				var scenarioID int
				fmt.Sscanf(scenarioIDStr, "%d", &scenarioID)
				if scenarioID == 0 {
					return fmt.Errorf("[step:%s] make_failed_executions requires scenario_id var", step.Name)
				}
				execs, err := makeConn.ListFailedExecutions(scenarioID, 10)
				if err != nil {
					return fmt.Errorf("[step:%s] make fetch failed executions failed: %w", step.Name, err)
				}
				if len(execs) == 0 {
					log.Printf("[pipeline:%s][step:%s] no failed executions, skipping", pipeline.Name, step.Name)
					return nil
				}
				data["failed_executions"] = FormatExecutionsForPrompt(execs)
				data["failure_count"] = fmt.Sprintf("%d", len(execs))
				log.Printf("[pipeline:%s][step:%s] fetched %d failed executions", pipeline.Name, step.Name, len(execs))

			case "make_get_blueprint":
				if makeConn == nil {
					return fmt.Errorf("[step:%s] make connector not configured", step.Name)
				}
				scenarioIDStr, _ := step.Vars["scenario_id"]
				var scenarioID int
				fmt.Sscanf(scenarioIDStr, "%d", &scenarioID)
				bp, err := makeConn.GetBlueprint(scenarioID)
				if err != nil {
					return fmt.Errorf("[step:%s] make get blueprint failed: %w", step.Name, err)
				}
				bpJSON, _ := json.MarshalIndent(bp, "", "  ")
				data["blueprint"] = string(bpJSON)
				data["blueprint_name"] = bp.Name
				log.Printf("[pipeline:%s][step:%s] fetched blueprint: %s", pipeline.Name, step.Name, bp.Name)

			// --- n8n actions ---

			case "n8n_list_workflows":
				if n8nConn == nil {
					return fmt.Errorf("[step:%s] n8n connector not configured", step.Name)
				}
				workflows, err := n8nConn.ListWorkflows()
				if err != nil {
					return fmt.Errorf("[step:%s] n8n list workflows failed: %w", step.Name, err)
				}
				data["workflows"] = FormatWorkflowsForPrompt(workflows)
				data["workflow_count"] = fmt.Sprintf("%d", len(workflows))
				log.Printf("[pipeline:%s][step:%s] fetched %d workflows", pipeline.Name, step.Name, len(workflows))

			case "n8n_failed_executions":
				if n8nConn == nil {
					return fmt.Errorf("[step:%s] n8n connector not configured", step.Name)
				}
				workflowID, _ := step.Vars["workflow_id"]
				execs, err := n8nConn.ListFailedExecutions(workflowID, 10)
				if err != nil {
					return fmt.Errorf("[step:%s] n8n fetch failed executions failed: %w", step.Name, err)
				}
				if len(execs) == 0 {
					log.Printf("[pipeline:%s][step:%s] no failed executions, skipping", pipeline.Name, step.Name)
					return nil
				}
				data["failed_executions"] = FormatN8NExecutionsForPrompt(execs)
				data["failure_count"] = fmt.Sprintf("%d", len(execs))
				log.Printf("[pipeline:%s][step:%s] fetched %d failed executions", pipeline.Name, step.Name, len(execs))

			case "n8n_get_workflow":
				if n8nConn == nil {
					return fmt.Errorf("[step:%s] n8n connector not configured", step.Name)
				}
				workflowID, _ := step.Vars["workflow_id"]
				if workflowID == "" {
					return fmt.Errorf("[step:%s] n8n_get_workflow requires workflow_id var", step.Name)
				}
				wf, err := n8nConn.GetWorkflow(workflowID)
				if err != nil {
					return fmt.Errorf("[step:%s] n8n get workflow failed: %w", step.Name, err)
				}
				wfJSON, _ := json.MarshalIndent(wf, "", "  ")
				data["workflow"] = string(wfJSON)
				data["workflow_name"] = wf.Name
				log.Printf("[pipeline:%s][step:%s] fetched workflow: %s", pipeline.Name, step.Name, wf.Name)

			// --- Notion actions ---

			case "notion_query_database":
				if notionConn == nil {
					return fmt.Errorf("[step:%s] notion connector not configured", step.Name)
				}
				dbID, _ := step.Vars["database_id"]
				if dbID == "" {
					return fmt.Errorf("[step:%s] notion_query_database requires database_id var", step.Name)
				}
				var filter json.RawMessage
				if f, ok := step.Vars["filter"]; ok && f != "" {
					filter = json.RawMessage(f)
				}
				pages, err := notionConn.QueryDatabase(dbID, filter, 20)
				if err != nil {
					return fmt.Errorf("[step:%s] notion query database failed: %w", step.Name, err)
				}
				if len(pages) == 0 {
					log.Printf("[pipeline:%s][step:%s] no pages found, skipping", pipeline.Name, step.Name)
					return nil
				}
				data["pages"] = FormatNotionPagesForPrompt(pages)
				data["page_count"] = fmt.Sprintf("%d", len(pages))
				log.Printf("[pipeline:%s][step:%s] fetched %d pages", pipeline.Name, step.Name, len(pages))

			case "notion_list_databases":
				if notionConn == nil {
					return fmt.Errorf("[step:%s] notion connector not configured", step.Name)
				}
				dbs, err := notionConn.ListDatabases()
				if err != nil {
					return fmt.Errorf("[step:%s] notion list databases failed: %w", step.Name, err)
				}
				data["databases"] = FormatNotionDatabasesForPrompt(dbs)
				data["database_count"] = fmt.Sprintf("%d", len(dbs))
				log.Printf("[pipeline:%s][step:%s] fetched %d databases", pipeline.Name, step.Name, len(dbs))

			case "notify":
				msg := ""
				if output, ok := data["ai_output"]; ok {
					switch v := output.(type) {
					case map[string]interface{}:
						raw, _ := json.MarshalIndent(v, "", "  ")
						msg = string(raw)
					case string:
						msg = v
					default:
						msg = fmt.Sprintf("%v", v)
					}
				} else if raw, ok := data["ai_raw"]; ok {
					msg = fmt.Sprintf("%v", raw)
				}
				if msg != "" {
					header := fmt.Sprintf("[%s] ", pipeline.Name)
					ch.Send(header + msg)
				}

			default:
				// No action -- pass-through
			}

		case "ai":
			if err := budget.check(cfg.Budgets.PerDayTokens, cfg.Budgets.PerStepTokens); err != nil {
				log.Printf("[pipeline:%s][step:%s] %s", pipeline.Name, step.Name, err)
				return err
			}

			prompt := step.Prompt
			role := step.Role

			if step.Skill != "" {
				skill, ok := skills.Get(step.Skill)
				if !ok {
					return fmt.Errorf("[step:%s] unknown skill: %s", step.Name, step.Skill)
				}
				prompt = skill.Prompt
				if skill.Role != "" {
					role = skill.Role
				}
			}

			// Template substitution
			for k, v := range data {
				if s, ok := v.(string); ok {
					prompt = strings.ReplaceAll(prompt, "{{"+k+"}}", s)
				}
			}
			for k, v := range step.Vars {
				prompt = strings.ReplaceAll(prompt, "{{"+k+"}}", v)
			}

			modelName := role
			if modelName == "" {
				modelName = "haiku"
			}
			model, ok := cfg.Models[modelName]
			if !ok {
				// Fall back to first model
				for _, m := range cfg.Models {
					model = m
					break
				}
			}

			aiResp, tokens, err := callLLM(cfg.Provider, model, prompt)
			if err != nil {
				return fmt.Errorf("[step:%s] LLM call failed: %w", step.Name, err)
			}

			budget.record(tokens)
			data["ai_raw"] = aiResp

			// Try to parse as JSON
			var parsed map[string]interface{}
			cleaned := strings.TrimSpace(aiResp)
			cleaned = strings.TrimPrefix(cleaned, "```json")
			cleaned = strings.TrimPrefix(cleaned, "```")
			cleaned = strings.TrimSuffix(cleaned, "```")
			cleaned = strings.TrimSpace(cleaned)

			if err := json.Unmarshal([]byte(cleaned), &parsed); err == nil {
				data["ai_output"] = parsed
			} else {
				data["ai_output"] = cleaned
			}

			log.Printf("[pipeline:%s][step:%s] LLM returned %d tokens", pipeline.Name, step.Name, tokens)

		case "approval":
			output := ""
			if v, ok := data["ai_raw"]; ok {
				output = fmt.Sprintf("%v", v)
			}

			decision, err := ch.WaitForApproval(ctx, fmt.Sprintf("[%s] Review:\n\n%s", pipeline.Name, output))
			if err != nil {
				return fmt.Errorf("[step:%s] approval failed: %w", step.Name, err)
			}

			if decision == "reject" || decision == "timeout" {
				log.Printf("[pipeline:%s][step:%s] %s by operator", pipeline.Name, step.Name, decision)
				return nil
			}

			data["approval"] = decision
			log.Printf("[pipeline:%s][step:%s] approved", pipeline.Name, step.Name)
		}
	}

	log.Printf("[pipeline:%s] completed", pipeline.Name)
	return nil
}

// --- LLM ---

func callLLM(provider ProviderConfig, model ModelConfig, prompt string) (string, int, error) {
	body := map[string]interface{}{
		"model":      model.Model,
		"max_tokens": model.MaxTokens,
		"messages": []map[string]string{
			{"role": "user", "content": prompt},
		},
	}

	data, err := json.Marshal(body)
	if err != nil {
		return "", 0, err
	}

	url := strings.TrimRight(provider.BaseURL, "/") + "/chat/completions"
	req, err := http.NewRequest("POST", url, bytes.NewReader(data))
	if err != nil {
		return "", 0, err
	}

	req.Header.Set("Authorization", "Bearer "+provider.apiKey)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("LLM request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", 0, err
	}

	if resp.StatusCode != 200 {
		return "", 0, fmt.Errorf("LLM returned %d: %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		Usage struct {
			TotalTokens int `json:"total_tokens"`
		} `json:"usage"`
	}

	if err := json.Unmarshal(respBody, &result); err != nil {
		return "", 0, err
	}

	if len(result.Choices) == 0 {
		return "", 0, fmt.Errorf("LLM returned no choices")
	}

	return result.Choices[0].Message.Content, result.Usage.TotalTokens, nil
}

// --- Scheduler ---

type Scheduler struct {
	pipelines []PipelineConfig
	mu        sync.Mutex
	lastRun   map[string]time.Time
}

func NewScheduler(pipelines []PipelineConfig) *Scheduler {
	return &Scheduler{
		pipelines: pipelines,
		lastRun:   make(map[string]time.Time),
	}
}

func (s *Scheduler) GetDue() []PipelineConfig {
	s.mu.Lock()
	defer s.mu.Unlock()

	var due []PipelineConfig
	now := time.Now()

	for _, p := range s.pipelines {
		if p.Schedule == "manual" || p.Schedule == "" {
			continue
		}

		interval, err := time.ParseDuration(p.Schedule)
		if err != nil {
			continue
		}

		last, ok := s.lastRun[p.Name]
		if !ok || now.Sub(last) >= interval {
			due = append(due, p)
			s.lastRun[p.Name] = now
		}
	}

	return due
}

// --- Main ---

func main() {
	// CLI flags
	configFlag := flag.String("config", "config.yaml", "path to config file")
	runFlag := flag.String("run", "", "run a single pipeline by name and exit")
	listFlag := flag.Bool("list", false, "list available pipelines and exit")
	flag.Parse()

	configPath := *configFlag

	data, err := os.ReadFile(configPath)
	if err != nil {
		log.Fatalf("failed to read config: %v", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("failed to parse config: %v", err)
	}

	// --- List mode ---
	if *listFlag {
		fmt.Printf("Available pipelines (%d):\n\n", len(cfg.Pipelines))
		for _, p := range cfg.Pipelines {
			fmt.Printf("  %-24s schedule: %s  steps: %d\n", p.Name, p.Schedule, len(p.Steps))
		}
		return
	}

	// Load secrets
	cfg.Provider.apiKey = os.Getenv(cfg.Provider.APIKeyEnv)
	if cfg.Provider.apiKey == "" {
		log.Fatalf("provider API key env var %s not set", cfg.Provider.APIKeyEnv)
	}

	cfg.Telegram.token = os.Getenv(cfg.Telegram.TokenEnv)

	// Load skills
	skillsDir := filepath.Join(filepath.Dir(configPath), "skills")
	skillReg, _ := loadSkills(skillsDir)

	// Init Make.com connector
	if cfg.Make.APIKeyEnv != "" {
		apiKey := os.Getenv(cfg.Make.APIKeyEnv)
		if apiKey != "" {
			makeConn, err = NewMakeConnector(cfg.Make, apiKey)
			if err != nil {
				log.Printf("[make] WARNING: failed to initialize: %v", err)
			}
		}
	}

	// Init n8n connector
	if cfg.N8N.BaseURL != "" {
		apiKey := os.Getenv(cfg.N8N.APIKeyEnv)
		if apiKey != "" {
			n8nConn, err = NewN8NConnector(cfg.N8N, apiKey)
			if err != nil {
				log.Printf("[n8n] WARNING: failed to initialize: %v", err)
			}
		}
	}

	// Init Notion connector
	if cfg.Notion.APIKeyEnv != "" {
		apiKey := os.Getenv(cfg.Notion.APIKeyEnv)
		if apiKey != "" {
			notionConn, err = NewNotionConnector(cfg.Notion, apiKey)
			if err != nil {
				log.Printf("[notion] WARNING: failed to initialize: %v", err)
			}
		}
	}

	// Init operator channel
	var ch OperatorChannel
	if cfg.Telegram.token != "" {
		ch = &TelegramChannel{
			token:  cfg.Telegram.token,
			chatID: cfg.Telegram.ChatID,
		}
		log.Printf("[telegram] operator channel configured")
	} else {
		log.Printf("[telegram] no token set — approval steps will timeout")
		ch = &TelegramChannel{} // stub, approval steps will timeout
	}

	budget := &Budget{dayStart: time.Now()}

	// --- One-shot mode ---
	if *runFlag != "" {
		var target *PipelineConfig
		for i := range cfg.Pipelines {
			if cfg.Pipelines[i].Name == *runFlag {
				target = &cfg.Pipelines[i]
				break
			}
		}
		if target == nil {
			log.Fatalf("pipeline %q not found. Use --list to see available pipelines.", *runFlag)
		}

		ctx, cancel := context.WithCancel(context.Background())
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		go func() { <-sigCh; cancel() }()

		log.Printf("[redaktflow] running pipeline: %s", target.Name)
		if err := runPipeline(ctx, *target, cfg, skillReg, ch, budget); err != nil {
			log.Fatalf("[pipeline:%s] error: %v", target.Name, err)
		}
		log.Printf("[redaktflow] pipeline %s completed", target.Name)
		return
	}

	// --- Daemon mode ---
	scheduler := NewScheduler(cfg.Pipelines)

	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("shutting down...")
		cancel()
	}()

	log.Printf("[redaktflow] started with %d pipelines (daemon mode)", len(cfg.Pipelines))

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[redaktflow] shutdown complete")
			return
		case <-ticker.C:
			for _, pipeline := range scheduler.GetDue() {
				go func(p PipelineConfig) {
					if err := runPipeline(ctx, p, cfg, skillReg, ch, budget); err != nil {
						log.Printf("[pipeline:%s] error: %v", p.Name, err)
						ch.Send(fmt.Sprintf("[%s] pipeline error: %v", p.Name, err))
					}
				}(pipeline)
			}
		}
	}
}
