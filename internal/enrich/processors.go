package enrich

import (
	"context"
	"fmt"
	"strings"
	"unicode"
)

const defaultModelVersion = "local-skeleton-v1"

type ProcessorConfig struct {
	ModelVersion string
}

func NewProcessors(tasks []string, cfg ProcessorConfig) ([]Processor, error) {
	tasks = NormalizeTasks(tasks)
	processors := make([]Processor, 0, len(tasks))
	for _, task := range tasks {
		switch task {
		case TaskQuality:
			processors = append(processors, NewContributionQualityProcessor(cfg.ModelVersion))
		default:
			return nil, fmt.Errorf("unsupported enrichment task %q", task)
		}
	}
	return processors, nil
}

func NormalizeTasks(tasks []string) []string {
	out := make([]string, 0, len(tasks))
	seen := make(map[string]struct{}, len(tasks))
	for _, task := range tasks {
		task = strings.TrimSpace(strings.ToLower(task))
		if task == "" || task == "none" || task == "off" || task == "disabled" {
			continue
		}
		if _, ok := seen[task]; ok {
			continue
		}
		seen[task] = struct{}{}
		out = append(out, task)
	}
	return out
}

func SupportedTask(task string) bool {
	switch strings.TrimSpace(strings.ToLower(task)) {
	case "", "none", "off", "disabled", TaskQuality:
		return true
	default:
		return false
	}
}

type ContributionQualityProcessor struct {
	modelVersion string
}

func NewContributionQualityProcessor(modelVersion string) *ContributionQualityProcessor {
	return &ContributionQualityProcessor{modelVersion: modelVersionOrDefault(modelVersion)}
}

func (p *ContributionQualityProcessor) Task() string {
	return TaskQuality
}

func (p *ContributionQualityProcessor) ProcessBatch(ctx context.Context, events []Event) ([]ProcessResult, error) {
	results := make([]ProcessResult, len(events))
	for i, event := range events {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		if !isReplyEvent(event) {
			continue
		}
		quality := contributionQuality(event.Content)
		results[i] = ProcessResult{
			Annotation: Annotation{
				Metrics: []Metric{
					{Name: "contribution_quality", Value: quality.Total},
					{Name: "reply_relevance", Value: quality.Relevance},
					{Name: "reply_informativeness", Value: quality.Informativeness},
					{Name: "reply_novelty", Value: quality.Novelty},
					{Name: "reply_constructiveness", Value: quality.Constructiveness},
				},
				ModelVersion: p.modelVersion,
			},
		}
	}
	return results, nil
}

type qualityScores struct {
	Total            float64
	Relevance        float64
	Informativeness  float64
	Novelty          float64
	Constructiveness float64
}

func isReplyEvent(event Event) bool {
	if event.Kind != 1 && event.Kind != 1111 {
		return false
	}
	for _, tag := range event.Tags {
		if len(tag) < 2 || strings.ToLower(tag[0]) != "e" {
			continue
		}
		if len(tag) >= 4 {
			marker := strings.ToLower(strings.TrimSpace(tag[3]))
			if marker == "reply" || marker == "root" {
				return true
			}
		}
		return true
	}
	return false
}

func sentimentScore(content string) float64 {
	lower := strings.ToLower(content)
	positive := countMatches(lower, []string{
		"good", "great", "excellent", "love", "like", "useful", "helpful", "agree", "correct",
	})
	negative := countMatches(lower, []string{
		"bad", "terrible", "hate", "wrong", "broken", "spam", "scam", "awful", "disagree",
	})
	total := positive + negative
	if total == 0 {
		return 0
	}
	return clamp(float64(positive-negative)/float64(total), -1, 1)
}

func contributionQuality(content string) qualityScores {
	toks := tokens(content)
	tokenCount := len(toks)
	uniqueTokens := uniqueTokenRatio(toks)
	lengthScore := clamp(float64(tokenCount)/36, 0, 1)
	relevance := 0.55
	if tokenCount >= 4 {
		relevance += 0.15
	}
	if containsAny(strings.ToLower(content), []string{"because", "therefore", "since", "example", "evidence", "source"}) {
		relevance += 0.15
	}
	relevance = clamp(relevance, 0, 1)

	informativeness := clamp((lengthScore*0.65)+(uniqueTokens*0.35), 0, 1)
	novelty := clamp(0.35+(uniqueTokens*0.65), 0, 1)
	constructiveness := clamp(0.65+(sentimentScore(content)*0.2), 0, 1)

	lower := strings.ToLower(content)
	if isReactionOnly(content) {
		informativeness *= 0.25
		constructiveness *= 0.75
	}
	if repeatedTokenPenalty(toks) > 0.45 {
		novelty *= 0.45
		informativeness *= 0.75
	}
	if containsAny(lower, []string{"idiot", "moron", "stupid", "shut up", "kill yourself"}) {
		constructiveness *= 0.25
	}
	if containsAny(lower, []string{"airdrop", "giveaway", "click here", "free money", "telegram"}) {
		informativeness *= 0.5
		novelty *= 0.6
	}

	total := (relevance * 0.25) + (informativeness * 0.35) + (novelty * 0.15) + (constructiveness * 0.25)
	return qualityScores{
		Total:            clamp(total, 0, 1),
		Relevance:        relevance,
		Informativeness:  informativeness,
		Novelty:          novelty,
		Constructiveness: constructiveness,
	}
}

func containsAny(value string, needles []string) bool {
	for _, needle := range needles {
		if strings.Contains(value, needle) {
			return true
		}
	}
	return false
}

func countMatches(value string, needles []string) int {
	var count int
	for _, needle := range needles {
		if strings.Contains(value, needle) {
			count++
		}
	}
	return count
}

func isReactionOnly(content string) bool {
	toks := tokens(content)
	if len(toks) == 0 {
		return strings.TrimSpace(content) != ""
	}
	if len(toks) > 3 {
		return false
	}
	return containsAny(strings.ToLower(content), []string{"lol", "haha", "wow", "nice", "gm", "ngmi", "lfg"})
}

func uniqueTokenRatio(toks []string) float64 {
	if len(toks) == 0 {
		return 0
	}
	seen := map[string]struct{}{}
	for _, token := range toks {
		seen[token] = struct{}{}
	}
	return float64(len(seen)) / float64(len(toks))
}

func repeatedTokenPenalty(toks []string) float64 {
	if len(toks) == 0 {
		return 0
	}
	counts := map[string]int{}
	var maxCount int
	for _, token := range toks {
		counts[token]++
		if counts[token] > maxCount {
			maxCount = counts[token]
		}
	}
	return float64(maxCount) / float64(len(toks))
}

func clamp(value, min, max float64) float64 {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

func tokens(content string) []string {
	parts := strings.FieldsFunc(strings.ToLower(content), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if len(part) < 2 {
			continue
		}
		out = append(out, part)
	}
	return out
}

func modelVersionOrDefault(version string) string {
	version = strings.TrimSpace(version)
	if version == "" {
		return defaultModelVersion
	}
	return version
}
