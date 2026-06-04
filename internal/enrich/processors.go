package enrich

import (
	"context"
	"fmt"
	"hash/fnv"
	"math"
	"net/url"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"
)

const defaultModelVersion = "local-skeleton-v1"
const defaultTrendingDedupeSimilarity = 0.82

type ProcessorConfig struct {
	ModelDir                 string
	ModelVersion             string
	ModelBackend             string
	OnnxLibraryPath          string
	TrendingDedupeSimilarity float64
	ModelProvider            ModelProvider
}

func NewProcessors(tasks []string, cfg ProcessorConfig) ([]Processor, error) {
	tasks = NormalizeTasks(tasks)
	modelProvider := cfg.ModelProvider
	if modelProvider == nil && strings.TrimSpace(cfg.ModelDir) != "" {
		modelProvider = NewHugotModelProvider(HugotModelConfig{
			ModelDir:        cfg.ModelDir,
			Backend:         cfg.ModelBackend,
			OnnxLibraryPath: cfg.OnnxLibraryPath,
		})
	}
	processors := make([]Processor, 0, len(tasks))
	for _, task := range tasks {
		switch task {
		case TaskTopics:
			processors = append(processors, NewKeywordTopicProcessor(cfg.ModelVersion))
		case TaskEmbeddings:
			processors = append(processors, NewHashingEmbeddingProcessor(cfg.ModelVersion, 384, modelProvider))
		case TaskTrending:
			processors = append(processors, NewTrendingProcessorWithSimilarity(cfg.ModelVersion, cfg.TrendingDedupeSimilarity, modelProvider))
		case TaskStance:
			processors = append(processors, NewStanceProcessor(cfg.ModelVersion, modelProvider))
		case TaskSentiment:
			processors = append(processors, NewSentimentProcessor(cfg.ModelVersion, modelProvider))
		case TaskQuality:
			processors = append(processors, NewContributionQualityProcessor(cfg.ModelVersion, modelProvider))
		case TaskControversy:
			processors = append(processors, NewControversyProcessor(cfg.ModelVersion, modelProvider))
		case TaskNSFW:
			processors = append(processors, NewNSFWProcessor(cfg.ModelVersion, modelProvider))
		default:
			return nil, fmt.Errorf("unsupported enrichment task %q", task)
		}
	}
	return processors, nil
}

func CloseProcessors(processors []Processor) error {
	seen := map[ModelProvider]struct{}{}
	for _, processor := range processors {
		owner, ok := processor.(interface{ modelProvider() ModelProvider })
		if !ok {
			continue
		}
		models := owner.modelProvider()
		if models == nil {
			continue
		}
		if _, ok := seen[models]; ok {
			continue
		}
		seen[models] = struct{}{}
		if err := models.Close(); err != nil {
			return err
		}
	}
	return nil
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
	case "", "none", "off", "disabled",
		TaskTopics, TaskEmbeddings, TaskTrending,
		TaskStance, TaskSentiment, TaskQuality, TaskControversy, TaskNSFW:
		return true
	default:
		return false
	}
}

type KeywordTopicProcessor struct {
	modelVersion string
	rules        []topicRule
}

type topicRule struct {
	value    string
	keywords []string
}

func NewKeywordTopicProcessor(modelVersion string) *KeywordTopicProcessor {
	return &KeywordTopicProcessor{
		modelVersion: modelVersionOrDefault(modelVersion),
		rules: []topicRule{
			{value: "crypto.bitcoin", keywords: []string{"bitcoin", "btc", "sats", "satoshi"}},
			{value: "protocol.nostr", keywords: []string{"nostr", "npub", "nprofile", "relay", "nip-"}},
			{value: "payments.cashu", keywords: []string{"cashu", "ecash", "mint", "nut-"}},
			{value: "payments.lightning", keywords: []string{"lightning", "bolt11", "lnbc"}},
			{value: "technology.ai", keywords: []string{"ai", "ml", "model", "embedding", "onnx"}},
		},
	}
}

func (p *KeywordTopicProcessor) Task() string {
	return TaskTopics
}

func (p *KeywordTopicProcessor) ProcessBatch(ctx context.Context, events []Event) ([]ProcessResult, error) {
	results := make([]ProcessResult, len(events))
	for i, event := range events {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		tags := p.topicTags(event)
		results[i] = ProcessResult{
			Annotation: Annotation{
				Tags:         tags,
				ModelVersion: p.modelVersion,
			},
		}
	}
	return results, nil
}

func (p *KeywordTopicProcessor) topicTags(event Event) []Tag {
	values := map[string]struct{}{}
	content := strings.ToLower(event.Content)
	for _, rule := range p.rules {
		for _, keyword := range rule.keywords {
			if strings.Contains(content, keyword) {
				values[rule.value] = struct{}{}
				break
			}
		}
	}
	for _, tag := range event.Tags {
		if len(tag) < 2 || strings.ToLower(tag[0]) != "t" {
			continue
		}
		switch strings.Trim(strings.ToLower(tag[1]), "# ") {
		case "bitcoin", "btc", "sats":
			values["crypto.bitcoin"] = struct{}{}
		case "nostr":
			values["protocol.nostr"] = struct{}{}
		case "cashu", "ecash":
			values["payments.cashu"] = struct{}{}
		case "lightning":
			values["payments.lightning"] = struct{}{}
		case "ai", "ml":
			values["technology.ai"] = struct{}{}
		}
	}
	if len(values) == 0 {
		return nil
	}
	ordered := make([]string, 0, len(values))
	for value := range values {
		ordered = append(ordered, value)
	}
	sort.Strings(ordered)
	tags := make([]Tag, 0, len(ordered))
	for _, value := range ordered {
		tags = append(tags, Tag{Key: "topic", Value: value})
	}
	return tags
}

type HashingEmbeddingProcessor struct {
	modelVersion string
	dimensions   int
	models       ModelProvider
}

func NewHashingEmbeddingProcessor(modelVersion string, dimensions int, models ...ModelProvider) *HashingEmbeddingProcessor {
	if dimensions <= 0 {
		dimensions = 384
	}
	var modelProvider ModelProvider
	if len(models) > 0 {
		modelProvider = models[0]
	}
	return &HashingEmbeddingProcessor{
		modelVersion: modelVersionOrDefault(modelVersion),
		dimensions:   dimensions,
		models:       modelProvider,
	}
}

func (p *HashingEmbeddingProcessor) Task() string {
	return TaskEmbeddings
}

func (p *HashingEmbeddingProcessor) modelProvider() ModelProvider {
	return p.models
}

func (p *HashingEmbeddingProcessor) ProcessBatch(ctx context.Context, events []Event) ([]ProcessResult, error) {
	if p.models != nil {
		inputs := make([]string, len(events))
		for i, event := range events {
			inputs[i] = event.Content
		}
		embeddings, ok, err := p.models.Embed(ctx, inputs)
		if err != nil {
			return nil, err
		}
		if ok {
			results := make([]ProcessResult, len(events))
			for i, embedding := range embeddings {
				results[i] = ProcessResult{
					Annotation: Annotation{
						Embedding:    embedding,
						ModelVersion: p.modelVersion,
					},
				}
			}
			return results, nil
		}
	}
	results := make([]ProcessResult, len(events))
	for i, event := range events {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		results[i] = ProcessResult{
			Annotation: Annotation{
				Embedding:    p.embed(event.Content),
				ModelVersion: p.modelVersion,
			},
		}
	}
	return results, nil
}

func (p *HashingEmbeddingProcessor) embed(content string) []float32 {
	tokens := tokens(content)
	if len(tokens) == 0 {
		return nil
	}
	vector := make([]float32, p.dimensions)
	for _, token := range tokens {
		hash := fnv64(token)
		index := int(hash % uint64(p.dimensions))
		sign := float32(1)
		if hash&1 == 1 {
			sign = -1
		}
		vector[index] += sign
	}
	var sum float64
	for _, value := range vector {
		sum += float64(value * value)
	}
	if sum == 0 {
		return nil
	}
	scale := float32(1 / math.Sqrt(sum))
	for i := range vector {
		vector[i] *= scale
	}
	return vector
}

type StanceProcessor struct {
	modelVersion string
	models       ModelProvider
}

func NewStanceProcessor(modelVersion string, models ...ModelProvider) *StanceProcessor {
	var modelProvider ModelProvider
	if len(models) > 0 {
		modelProvider = models[0]
	}
	return &StanceProcessor{modelVersion: modelVersionOrDefault(modelVersion), models: modelProvider}
}

func (p *StanceProcessor) Task() string {
	return TaskStance
}

func (p *StanceProcessor) modelProvider() ModelProvider {
	return p.models
}

func (p *StanceProcessor) ProcessBatch(ctx context.Context, events []Event) ([]ProcessResult, error) {
	results := make([]ProcessResult, len(events))
	replyIndexes := []int{}
	replyInputs := []string{}
	for i, event := range events {
		if isReplyEvent(event) {
			replyIndexes = append(replyIndexes, i)
			replyInputs = append(replyInputs, event.Content)
		}
	}
	if p.models != nil && len(replyInputs) > 0 {
		classifications, ok, err := p.models.ClassifyStance(ctx, replyInputs, stanceLabels())
		if err != nil {
			return nil, err
		}
		if ok {
			for i, labels := range classifications {
				if i >= len(replyIndexes) {
					break
				}
				results[replyIndexes[i]] = ProcessResult{
					Annotation: Annotation{
						Tags:         []Tag{{Key: "stance", Value: bestAllowedLabel(labels, stanceLabels(), stanceLabel(replyInputs[i]))}},
						ModelVersion: p.modelVersion,
					},
				}
			}
			return results, nil
		}
	}
	for i, event := range events {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		if !isReplyEvent(event) {
			continue
		}
		results[i] = ProcessResult{
			Annotation: Annotation{
				Tags:         []Tag{{Key: "stance", Value: stanceLabel(event.Content)}},
				ModelVersion: p.modelVersion,
			},
		}
	}
	return results, nil
}

type SentimentProcessor struct {
	modelVersion string
	models       ModelProvider
}

func NewSentimentProcessor(modelVersion string, models ...ModelProvider) *SentimentProcessor {
	var modelProvider ModelProvider
	if len(models) > 0 {
		modelProvider = models[0]
	}
	return &SentimentProcessor{modelVersion: modelVersionOrDefault(modelVersion), models: modelProvider}
}

func (p *SentimentProcessor) Task() string {
	return TaskSentiment
}

func (p *SentimentProcessor) modelProvider() ModelProvider {
	return p.models
}

func (p *SentimentProcessor) ProcessBatch(ctx context.Context, events []Event) ([]ProcessResult, error) {
	results := make([]ProcessResult, len(events))
	inputIndexes := []int{}
	inputs := []string{}
	for i, event := range events {
		if event.Kind == 1 || event.Kind == 1111 {
			inputIndexes = append(inputIndexes, i)
			inputs = append(inputs, event.Content)
		}
	}
	if p.models != nil && len(inputs) > 0 {
		classifications, ok, err := p.models.ClassifySentiment(ctx, inputs)
		if err != nil {
			return nil, err
		}
		if ok {
			for i, labels := range classifications {
				if i >= len(inputIndexes) {
					break
				}
				results[inputIndexes[i]] = ProcessResult{
					Annotation: Annotation{
						Metrics:      []Metric{{Name: "sentiment", Value: sentimentFromLabels(labels, inputs[i])}},
						ModelVersion: p.modelVersion,
					},
				}
			}
			return results, nil
		}
	}
	for i, event := range events {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		if event.Kind != 1 && event.Kind != 1111 {
			continue
		}
		results[i] = ProcessResult{
			Annotation: Annotation{
				Metrics:      []Metric{{Name: "sentiment", Value: sentimentScore(event.Content)}},
				ModelVersion: p.modelVersion,
			},
		}
	}
	return results, nil
}

type ContributionQualityProcessor struct {
	modelVersion string
	models       ModelProvider
}

func NewContributionQualityProcessor(modelVersion string, models ...ModelProvider) *ContributionQualityProcessor {
	var modelProvider ModelProvider
	if len(models) > 0 {
		modelProvider = models[0]
	}
	return &ContributionQualityProcessor{modelVersion: modelVersionOrDefault(modelVersion), models: modelProvider}
}

func (p *ContributionQualityProcessor) Task() string {
	return TaskQuality
}

func (p *ContributionQualityProcessor) modelProvider() ModelProvider {
	return p.models
}

func (p *ContributionQualityProcessor) ProcessBatch(ctx context.Context, events []Event) ([]ProcessResult, error) {
	results := make([]ProcessResult, len(events))
	replyIndexes := []int{}
	replyInputs := []string{}
	for i, event := range events {
		if isReplyEvent(event) {
			replyIndexes = append(replyIndexes, i)
			replyInputs = append(replyInputs, event.Content)
		}
	}
	var embeddings [][]float32
	var embeddingsOK bool
	if p.models != nil && len(replyInputs) > 0 {
		var err error
		embeddings, embeddingsOK, err = p.models.Embed(ctx, replyInputs)
		if err != nil {
			return nil, err
		}
	}
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
		if embeddingsOK {
			if replyIdx := indexOfInt(replyIndexes, i); replyIdx >= 0 && replyIdx < len(embeddings) {
				quality = quality.withEmbedding(embeddings[replyIdx])
			}
		}
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

type ControversyProcessor struct {
	modelVersion string
	models       ModelProvider
}

func NewControversyProcessor(modelVersion string, models ...ModelProvider) *ControversyProcessor {
	var modelProvider ModelProvider
	if len(models) > 0 {
		modelProvider = models[0]
	}
	return &ControversyProcessor{modelVersion: modelVersionOrDefault(modelVersion), models: modelProvider}
}

func (p *ControversyProcessor) Task() string {
	return TaskControversy
}

func (p *ControversyProcessor) modelProvider() ModelProvider {
	return p.models
}

func (p *ControversyProcessor) ProcessBatch(ctx context.Context, events []Event) ([]ProcessResult, error) {
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
		results[i] = ProcessResult{
			Annotation: Annotation{
				Metrics:      []Metric{{Name: "controversy", Value: controversyScore(event.Content)}},
				ModelVersion: p.modelVersion,
			},
		}
	}
	return results, nil
}

type NSFWProcessor struct {
	modelVersion string
	models       ModelProvider
}

func NewNSFWProcessor(modelVersion string, models ...ModelProvider) *NSFWProcessor {
	var modelProvider ModelProvider
	if len(models) > 0 {
		modelProvider = models[0]
	}
	return &NSFWProcessor{modelVersion: modelVersionOrDefault(modelVersion), models: modelProvider}
}

func (p *NSFWProcessor) Task() string {
	return TaskNSFW
}

func (p *NSFWProcessor) modelProvider() ModelProvider {
	return p.models
}

func (p *NSFWProcessor) ProcessBatch(ctx context.Context, events []Event) ([]ProcessResult, error) {
	results := make([]ProcessResult, len(events))
	mediaIndexes := []int{}
	textInputs := []string{}
	imagePaths := []string{}
	imagePathToIndex := []int{}
	for i, event := range events {
		if !containsMedia(event) {
			continue
		}
		mediaIndexes = append(mediaIndexes, i)
		textInputs = append(textInputs, event.Content)
		for _, path := range localMediaPaths(eventMediaRefs(event)) {
			imagePaths = append(imagePaths, path)
			imagePathToIndex = append(imagePathToIndex, i)
		}
	}
	if p.models != nil && len(mediaIndexes) > 0 {
		tagged := map[int]struct{}{}
		if len(textInputs) > 0 {
			classifications, ok, err := p.models.ClassifyNSFWText(ctx, textInputs)
			if err != nil {
				return nil, err
			}
			if ok {
				for i, labels := range classifications {
					if i < len(mediaIndexes) && labelsAreExplicit(labels) {
						tagged[mediaIndexes[i]] = struct{}{}
					}
				}
			}
		}
		if len(imagePaths) > 0 {
			classifications, ok, err := p.models.ClassifyNSFWImages(ctx, imagePaths)
			if err != nil {
				return nil, err
			}
			if ok {
				for i, labels := range classifications {
					if i < len(imagePathToIndex) && labelsAreExplicit(labels) {
						tagged[imagePathToIndex[i]] = struct{}{}
					}
				}
			}
		}
		if len(tagged) > 0 {
			for index := range tagged {
				results[index] = ProcessResult{
					Annotation: Annotation{
						Tags:         []Tag{{Key: "nsfw", Value: "explicit"}},
						ModelVersion: p.modelVersion,
					},
				}
			}
			return results, nil
		}
	}
	for i, event := range events {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		if !containsMedia(event) || !containsExplicitSignal(event) {
			continue
		}
		results[i] = ProcessResult{
			Annotation: Annotation{
				Tags:         []Tag{{Key: "nsfw", Value: "explicit"}},
				ModelVersion: p.modelVersion,
			},
		}
	}
	return results, nil
}

type TrendingProcessor struct {
	modelVersion     string
	topics           *KeywordTopicProcessor
	embedder         *HashingEmbeddingProcessor
	models           ModelProvider
	dedupeSimilarity float64
}

type trendingGroup struct {
	key           string
	window        string
	category      string
	subcategory   string
	startedAt     time.Time
	eventIdxs     []int
	centroidSum   []float32
	centroidCount int
}

type trendingWindowSpec struct {
	name     string
	duration time.Duration
}

func NewTrendingProcessor(modelVersion string, models ...ModelProvider) *TrendingProcessor {
	return NewTrendingProcessorWithSimilarity(modelVersion, defaultTrendingDedupeSimilarity, models...)
}

func NewTrendingProcessorWithSimilarity(modelVersion string, dedupeSimilarity float64, models ...ModelProvider) *TrendingProcessor {
	modelVersion = modelVersionOrDefault(modelVersion)
	var modelProvider ModelProvider
	if len(models) > 0 {
		modelProvider = models[0]
	}
	return &TrendingProcessor{
		modelVersion:     modelVersion,
		topics:           NewKeywordTopicProcessor(modelVersion),
		embedder:         NewHashingEmbeddingProcessor(modelVersion, 384, modelProvider),
		models:           modelProvider,
		dedupeSimilarity: normalizeTrendingDedupeSimilarity(dedupeSimilarity),
	}
}

func (p *TrendingProcessor) Task() string {
	return TaskTrending
}

func (p *TrendingProcessor) modelProvider() ModelProvider {
	return p.models
}

func (p *TrendingProcessor) ProcessBatch(ctx context.Context, events []Event) ([]ProcessResult, error) {
	results := make([]ProcessResult, len(events))
	embeddings, err := p.eventEmbeddings(ctx, events)
	if err != nil {
		return nil, err
	}
	groups := []*trendingGroup{}
	for i, event := range events {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		category, subcategory, ok := p.eventTopic(event)
		if !ok {
			continue
		}
		vector := embeddingAt(embeddings, i)
		for _, window := range trendingWindows() {
			startedAt := truncateWindow(event.CreatedAt, window.duration)
			group := p.matchTrendingGroup(groups, window.name, startedAt, category, subcategory, vector)
			if group == nil {
				key := window.name + "|" + startedAt.Format(time.RFC3339) + "|" + category + "|" + subcategory + fmt.Sprintf("|%d", len(groups))
				group = &trendingGroup{
					key:         key,
					window:      window.name,
					category:    category,
					subcategory: subcategory,
					startedAt:   startedAt,
				}
				groups = append(groups, group)
			}
			group.addEvent(i, vector)
		}
	}

	sort.Slice(groups, func(i, j int) bool {
		left := groups[i]
		right := groups[j]
		if windowRank(left.window) != windowRank(right.window) {
			return windowRank(left.window) < windowRank(right.window)
		}
		if !left.startedAt.Equal(right.startedAt) {
			return left.startedAt.Before(right.startedAt)
		}
		if left.category != right.category {
			return left.category < right.category
		}
		if left.subcategory != right.subcategory {
			return left.subcategory < right.subcategory
		}
		return left.key < right.key
	})
	for _, group := range groups {
		cluster := p.clusterForGroup(group)
		for _, idx := range group.eventIdxs {
			annotation := results[idx].Annotation
			annotation.Tags = append(annotation.Tags, Tag{Key: "cluster", Value: cluster.ID})
			annotation.ModelVersion = p.modelVersion
			results[idx].Annotation = annotation
		}
		firstIdx := group.eventIdxs[0]
		annotation := results[firstIdx].Annotation
		annotation.Clusters = append(annotation.Clusters, cluster)
		results[firstIdx].Annotation = annotation
	}
	return results, nil
}

func (p *TrendingProcessor) eventEmbeddings(ctx context.Context, events []Event) ([][]float32, error) {
	inputs := make([]string, len(events))
	for i, event := range events {
		inputs[i] = event.Content
	}
	if p.models != nil && len(inputs) > 0 {
		embeddings, ok, err := p.models.Embed(ctx, inputs)
		if err != nil {
			return nil, err
		}
		if ok && len(embeddings) == len(events) {
			return embeddings, nil
		}
	}
	embeddings := make([][]float32, len(events))
	for i, event := range events {
		embeddings[i] = p.embedder.embed(event.Content)
	}
	return embeddings, nil
}

func (p *TrendingProcessor) matchTrendingGroup(groups []*trendingGroup, window string, startedAt time.Time, category string, subcategory string, vector []float32) *trendingGroup {
	var best *trendingGroup
	bestSimilarity := math.Inf(-1)
	for _, group := range groups {
		if group.window != window || !group.startedAt.Equal(startedAt) || group.category != category || group.subcategory != subcategory {
			continue
		}
		centroid := group.centroid()
		if len(vector) == 0 || len(centroid) == 0 {
			if best == nil {
				best = group
			}
			continue
		}
		similarity := cosineSimilarity(centroid, vector)
		if similarity > bestSimilarity {
			bestSimilarity = similarity
			best = group
		}
	}
	if best == nil {
		return nil
	}
	if len(vector) == 0 || len(best.centroid()) == 0 {
		return best
	}
	if bestSimilarity >= p.dedupeSimilarity {
		return best
	}
	return nil
}

func (p *TrendingProcessor) eventTopic(event Event) (string, string, bool) {
	tags := p.topics.topicTags(event)
	if len(tags) == 0 {
		return "", "", false
	}
	value := tags[0].Value
	category, subcategory := splitTopic(value)
	if category == "" {
		return "", "", false
	}
	return category, subcategory, true
}

func (p *TrendingProcessor) clusterForGroup(group *trendingGroup) TrendingCluster {
	label := topicLabel(group.category, group.subcategory)
	centroid := group.centroid()
	id := p.clusterID(group, centroid)
	cluster := TrendingCluster{
		ID:          id,
		Window:      group.window,
		StartedAt:   group.startedAt,
		Category:    group.category,
		Subcategory: group.subcategory,
		Title:       label,
		Description: fmt.Sprintf("%d notes in %s about %s", len(group.eventIdxs), group.window, label),
		EventCount:  uint64(len(group.eventIdxs)),
		Score:       float64(len(group.eventIdxs)),
		Centroid:    centroid,
	}
	return cluster
}

func (p *TrendingProcessor) clusterID(group *trendingGroup, centroid []float32) string {
	fingerprint := roundedCentroidFingerprint(centroid)
	if fingerprint == "" {
		fingerprint = group.category + "|" + group.subcategory
	}
	value := strings.Join([]string{
		group.window,
		group.startedAt.Format(time.RFC3339),
		group.category,
		group.subcategory,
		fingerprint,
	}, "|")
	return fmt.Sprintf("cluster:%016x", fnv64(value))
}

func (group *trendingGroup) addEvent(idx int, vector []float32) {
	group.eventIdxs = append(group.eventIdxs, idx)
	if len(vector) == 0 {
		return
	}
	if group.centroidSum == nil {
		group.centroidSum = make([]float32, len(vector))
	}
	if len(group.centroidSum) != len(vector) {
		return
	}
	for i, value := range vector {
		group.centroidSum[i] += value
	}
	group.centroidCount++
}

func (group *trendingGroup) centroid() []float32 {
	if group.centroidCount == 0 || len(group.centroidSum) == 0 {
		return nil
	}
	centroid := make([]float32, len(group.centroidSum))
	scale := float32(1 / float64(group.centroidCount))
	var norm float64
	for i := range centroid {
		centroid[i] = group.centroidSum[i] * scale
		norm += float64(centroid[i] * centroid[i])
	}
	if norm == 0 {
		return nil
	}
	normScale := float32(1 / math.Sqrt(norm))
	for i := range centroid {
		centroid[i] *= normScale
	}
	return centroid
}

func trendingWindows() []trendingWindowSpec {
	return []trendingWindowSpec{
		{name: "H8", duration: 8 * time.Hour},
		{name: "H24", duration: 24 * time.Hour},
		{name: "D7", duration: 7 * 24 * time.Hour},
	}
}

func truncateWindow(value time.Time, duration time.Duration) time.Time {
	if duration <= 0 {
		duration = 24 * time.Hour
	}
	unix := value.UTC().Unix()
	windowSeconds := int64(duration.Seconds())
	if windowSeconds <= 0 {
		return value.UTC()
	}
	return time.Unix((unix/windowSeconds)*windowSeconds, 0).UTC()
}

func windowRank(window string) int {
	switch window {
	case "H8":
		return 0
	case "H24":
		return 1
	case "D7":
		return 2
	default:
		return 99
	}
}

func embeddingAt(embeddings [][]float32, index int) []float32 {
	if index < 0 || index >= len(embeddings) {
		return nil
	}
	return embeddings[index]
}

func cosineSimilarity(left []float32, right []float32) float64 {
	if len(left) == 0 || len(left) != len(right) {
		return -1
	}
	var dot float64
	var leftNorm float64
	var rightNorm float64
	for i := range left {
		dot += float64(left[i] * right[i])
		leftNorm += float64(left[i] * left[i])
		rightNorm += float64(right[i] * right[i])
	}
	if leftNorm == 0 || rightNorm == 0 {
		return -1
	}
	return dot / (math.Sqrt(leftNorm) * math.Sqrt(rightNorm))
}

func roundedCentroidFingerprint(centroid []float32) string {
	if len(centroid) == 0 {
		return ""
	}
	parts := make([]string, len(centroid))
	for i, value := range centroid {
		parts[i] = fmt.Sprintf("%d", int(math.Round(float64(value)*100)))
	}
	return strings.Join(parts, ",")
}

func normalizeTrendingDedupeSimilarity(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) || value <= 0 || value > 1 {
		return defaultTrendingDedupeSimilarity
	}
	return value
}

func splitTopic(value string) (string, string) {
	parts := strings.SplitN(strings.TrimSpace(value), ".", 2)
	if len(parts) == 0 || parts[0] == "" {
		return "", ""
	}
	if len(parts) == 1 {
		return parts[0], ""
	}
	return parts[0], parts[1]
}

func topicLabel(category string, subcategory string) string {
	value := strings.Trim(strings.ReplaceAll(category+" "+subcategory, ".", " "), " ")
	parts := strings.Fields(strings.ReplaceAll(value, "_", " "))
	for i, part := range parts {
		if part == "" {
			continue
		}
		parts[i] = strings.ToUpper(part[:1]) + part[1:]
	}
	if len(parts) == 0 {
		return "Trending"
	}
	return strings.Join(parts, " ")
}

type qualityScores struct {
	Total            float64
	Relevance        float64
	Informativeness  float64
	Novelty          float64
	Constructiveness float64
}

func (q qualityScores) withEmbedding(embedding []float32) qualityScores {
	if len(embedding) == 0 {
		return q
	}
	var norm float64
	var nonZero int
	for _, value := range embedding {
		if value != 0 {
			nonZero++
		}
		norm += float64(value * value)
	}
	norm = math.Sqrt(norm)
	q.Relevance = math.Max(q.Relevance, clamp(norm, 0, 1))
	q.Novelty = math.Max(q.Novelty, clamp(float64(nonZero)/float64(len(embedding))*8, 0, 1))
	q.Total = (q.Relevance * 0.25) + (q.Informativeness * 0.35) + (q.Novelty * 0.15) + (q.Constructiveness * 0.25)
	q.Total = clamp(q.Total, 0, 1)
	return q
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

func stanceLabel(content string) string {
	lower := strings.ToLower(content)
	agree := containsAny(lower, []string{"exactly", "+1", "makes sense"}) ||
		containsAnyWord(lower, []string{"agree", "yes", "correct", "right", "true"})
	disagree := containsAny(lower, []string{"not true", "hard disagree"}) ||
		containsAnyWord(lower, []string{"disagree", "wrong", "false", "no", "incorrect"})
	question := strings.Contains(lower, "?") || startsWithAny(lower, []string{
		"why ", "how ", "what ", "when ", "where ", "who ", "can ", "could ", "would ",
	})
	if isReactionOnly(content) {
		return "offtopic"
	}
	switch {
	case agree && disagree:
		return "mixed"
	case disagree:
		return "disagree"
	case question:
		return "question"
	case agree:
		return "agree"
	default:
		return "mixed"
	}
}

func stanceLabels() []string {
	return []string{"agree", "disagree", "mixed", "question", "offtopic"}
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

func sentimentFromLabels(labels []LabelScore, fallbackContent string) float64 {
	if len(labels) == 0 {
		return sentimentScore(fallbackContent)
	}
	var score float64
	var matched bool
	for _, labelScore := range labels {
		label := normalizeLabel(labelScore.Label)
		switch {
		case strings.Contains(label, "positive") || label == "label_2" || label == "2":
			score += labelScore.Score
			matched = true
		case strings.Contains(label, "negative") || label == "label_0" || label == "0":
			score -= labelScore.Score
			matched = true
		case strings.Contains(label, "neutral") || label == "label_1" || label == "1":
			matched = true
		}
	}
	if !matched {
		return sentimentScore(fallbackContent)
	}
	return clamp(score, -1, 1)
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

func controversyScore(content string) float64 {
	lower := strings.ToLower(content)
	score := 0.05
	if stanceLabel(content) == "disagree" {
		score += 0.35
	}
	score += math.Abs(sentimentScore(content)) * 0.25
	if containsAny(lower, []string{"but", "however", "actually", "debate", "controversial", "wrong"}) {
		score += 0.25
	}
	if strings.Contains(lower, "?") {
		score += 0.1
	}
	return clamp(score, 0, 1)
}

func containsMedia(event Event) bool {
	lowerContent := strings.ToLower(event.Content)
	if containsAny(lowerContent, []string{".jpg", ".jpeg", ".png", ".gif", ".webp", ".mp4", ".mov", ".m4v"}) {
		return true
	}
	for _, tag := range event.Tags {
		if len(tag) == 0 {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(tag[0]))
		if key == "imeta" || key == "image" || key == "video" {
			return true
		}
		for _, part := range tag[1:] {
			if containsAny(strings.ToLower(part), []string{".jpg", ".jpeg", ".png", ".gif", ".webp", ".mp4", ".mov", ".m4v"}) {
				return true
			}
		}
	}
	return false
}

func eventMediaRefs(event Event) []string {
	refs := []string{}
	for _, token := range strings.Fields(event.Content) {
		if mediaRef(token) {
			refs = append(refs, strings.Trim(token, " \t\r\n\"'()[]<>"))
		}
	}
	for _, tag := range event.Tags {
		for _, part := range tag[1:] {
			if mediaRef(part) {
				refs = append(refs, strings.Trim(part, " \t\r\n\"'()[]<>"))
			}
		}
	}
	return refs
}

func localMediaPaths(refs []string) []string {
	paths := []string{}
	for _, ref := range refs {
		ref = strings.TrimSpace(ref)
		if ref == "" {
			continue
		}
		if parsed, err := url.Parse(ref); err == nil && parsed.Scheme != "" && parsed.Scheme != "file" {
			continue
		}
		if strings.HasPrefix(ref, "file://") {
			parsed, err := url.Parse(ref)
			if err != nil {
				continue
			}
			ref = parsed.Path
		}
		if filepath.IsAbs(ref) {
			paths = append(paths, ref)
		}
	}
	return paths
}

func mediaRef(value string) bool {
	return containsAny(strings.ToLower(value), []string{".jpg", ".jpeg", ".png", ".gif", ".webp", ".mp4", ".mov", ".m4v"})
}

func containsExplicitSignal(event Event) bool {
	lower := strings.ToLower(event.Content)
	if containsAny(lower, []string{"nsfw", "porn", "nude", "nudity", "explicit", "xxx", "sex"}) {
		return true
	}
	for _, tag := range event.Tags {
		for _, part := range tag {
			if containsAny(strings.ToLower(part), []string{"nsfw", "porn", "nude", "explicit"}) {
				return true
			}
		}
	}
	return false
}

func labelsAreExplicit(labels []LabelScore) bool {
	var explicit float64
	var safe float64
	for _, labelScore := range labels {
		label := normalizeLabel(labelScore.Label)
		switch {
		case strings.Contains(label, "nsfw"),
			strings.Contains(label, "explicit"),
			strings.Contains(label, "porn"),
			strings.Contains(label, "unsafe"),
			strings.Contains(label, "sexual"),
			label == "label_1" || label == "1":
			explicit = math.Max(explicit, labelScore.Score)
		case strings.Contains(label, "safe"),
			strings.Contains(label, "sfw"),
			label == "label_0" || label == "0":
			safe = math.Max(safe, labelScore.Score)
		}
	}
	return explicit >= 0.5 && explicit >= safe
}

func bestAllowedLabel(labels []LabelScore, allowed []string, fallback string) string {
	if len(labels) == 0 {
		return fallback
	}
	allowedSet := map[string]struct{}{}
	for _, label := range allowed {
		allowedSet[normalizeLabel(label)] = struct{}{}
	}
	best := fallback
	bestScore := math.Inf(-1)
	for _, labelScore := range labels {
		label := normalizeLabel(labelScore.Label)
		if _, ok := allowedSet[label]; !ok {
			continue
		}
		if labelScore.Score > bestScore {
			bestScore = labelScore.Score
			best = label
		}
	}
	return best
}

func normalizeLabel(label string) string {
	return strings.ToLower(strings.TrimSpace(strings.ReplaceAll(label, "-", "_")))
}

func containsAny(value string, needles []string) bool {
	for _, needle := range needles {
		if strings.Contains(value, needle) {
			return true
		}
	}
	return false
}

func containsAnyWord(value string, words []string) bool {
	tokSet := map[string]struct{}{}
	for _, token := range tokens(value) {
		tokSet[token] = struct{}{}
	}
	for _, word := range words {
		if _, ok := tokSet[word]; ok {
			return true
		}
	}
	return false
}

func startsWithAny(value string, prefixes []string) bool {
	value = strings.TrimSpace(value)
	for _, prefix := range prefixes {
		if strings.HasPrefix(value, prefix) {
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

func indexOfInt(values []int, target int) int {
	for i, value := range values {
		if value == target {
			return i
		}
	}
	return -1
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

func fnv64(value string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(value))
	return h.Sum64()
}

func modelVersionOrDefault(version string) string {
	version = strings.TrimSpace(version)
	if version == "" {
		return defaultModelVersion
	}
	return version
}
