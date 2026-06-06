package enrich

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewProcessorsBuildsRequestedTasks(t *testing.T) {
	processors, err := NewProcessors([]string{
		TaskEmbeddings,
		TaskStance,
		TaskSentiment,
		TaskQuality,
		TaskControversy,
		TaskNSFW,
	}, ProcessorConfig{ModelVersion: "test-v1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(processors) != 6 {
		t.Fatalf("processors = %+v, want six processors", processors)
	}
	if processors[0].Task() != TaskEmbeddings || processors[5].Task() != TaskNSFW {
		t.Fatalf("processors = %+v, want requested task order", processors)
	}
}

func TestHashingEmbeddingProcessorReturnsNormalizedStableVector(t *testing.T) {
	processor := NewHashingEmbeddingProcessor("test-v1", 16)
	events := []Event{{Content: "nostr bitcoin bitcoin"}}
	first, err := processor.ProcessBatch(context.Background(), events)
	if err != nil {
		t.Fatal(err)
	}
	second, err := processor.ProcessBatch(context.Background(), events)
	if err != nil {
		t.Fatal(err)
	}
	vector := first[0].Annotation.Embedding
	if len(vector) != 16 {
		t.Fatalf("embedding dimensions = %d, want 16", len(vector))
	}
	var norm float64
	for i, value := range vector {
		if value != second[0].Annotation.Embedding[i] {
			t.Fatalf("embedding changed at %d: %f != %f", i, value, second[0].Annotation.Embedding[i])
		}
		norm += float64(value * value)
	}
	if math.Abs(norm-1) > 0.0001 {
		t.Fatalf("embedding norm = %f, want 1", norm)
	}
}

func TestPhase8ProcessorsWriteReplySignals(t *testing.T) {
	events := []Event{
		{
			ID:      hexID("1"),
			PubKey:  hexID("a"),
			Kind:    1,
			Content: "I disagree because the source says this is incorrect.",
			Tags:    [][]string{{"e", hexID("f"), "", "reply"}},
		},
		{
			ID:      hexID("2"),
			PubKey:  hexID("b"),
			Kind:    1,
			Content: "top level note with no reply tag",
		},
	}

	stance, err := NewStanceProcessor("test-v1").ProcessBatch(context.Background(), events)
	if err != nil {
		t.Fatal(err)
	}
	if len(stance[0].Annotation.Tags) != 1 || stance[0].Annotation.Tags[0].Value != "disagree" {
		t.Fatalf("stance tags = %+v, want disagree", stance[0].Annotation.Tags)
	}
	if !stance[1].Annotation.Empty() {
		t.Fatalf("top-level stance annotation = %+v, want empty", stance[1].Annotation)
	}

	quality, err := NewContributionQualityProcessor("test-v1").ProcessBatch(context.Background(), events)
	if err != nil {
		t.Fatal(err)
	}
	if metricValue(quality[0].Annotation.Metrics, "contribution_quality") <= 0.5 {
		t.Fatalf("quality metrics = %+v, want useful reply score", quality[0].Annotation.Metrics)
	}
	if metricValue(quality[0].Annotation.Metrics, "reply_constructiveness") <= 0 {
		t.Fatalf("quality metrics = %+v, want constructiveness score", quality[0].Annotation.Metrics)
	}

	controversy, err := NewControversyProcessor("test-v1").ProcessBatch(context.Background(), events)
	if err != nil {
		t.Fatal(err)
	}
	if metricValue(controversy[0].Annotation.Metrics, "controversy") <= 0.3 {
		t.Fatalf("controversy metrics = %+v, want disagreement score", controversy[0].Annotation.Metrics)
	}
}

func TestNSFWProcessorTagsOnlyExplicitMedia(t *testing.T) {
	events := []Event{
		{
			ID:      hexID("1"),
			PubKey:  hexID("a"),
			Kind:    1,
			Content: "nsfw https://example.com/image.jpg",
		},
		{
			ID:      hexID("2"),
			PubKey:  hexID("b"),
			Kind:    1,
			Content: "nsfw text without media",
		},
	}

	results, err := NewNSFWProcessor("test-v1").ProcessBatch(context.Background(), events)
	if err != nil {
		t.Fatal(err)
	}
	if len(results[0].Annotation.Tags) != 1 ||
		results[0].Annotation.Tags[0].Key != "nsfw" ||
		results[0].Annotation.Tags[0].Value != "explicit" {
		t.Fatalf("first nsfw tags = %+v, want explicit", results[0].Annotation.Tags)
	}
	if !results[1].Annotation.Empty() {
		t.Fatalf("second nsfw annotation = %+v, want empty", results[1].Annotation)
	}
}

func TestProcessorsUseModelProviderWhenAvailable(t *testing.T) {
	provider := &fakeModelProvider{
		embeddings: [][]float32{{1, 0}, {0, 1}},
		stance: [][]LabelScore{{
			{Label: "agree", Score: 0.9},
			{Label: "disagree", Score: 0.1},
		}},
		sentiment: [][]LabelScore{{
			{Label: "positive", Score: 0.8},
			{Label: "negative", Score: 0.2},
		}},
		nsfwText: [][]LabelScore{{
			{Label: "explicit", Score: 0.95},
			{Label: "safe", Score: 0.05},
		}},
	}
	events := []Event{
		{
			ID:      hexID("1"),
			PubKey:  hexID("a"),
			Kind:    1,
			Content: "I agree because this example adds useful detail.",
			Tags:    [][]string{{"e", hexID("f"), "", "reply"}},
		},
		{
			ID:      hexID("2"),
			PubKey:  hexID("b"),
			Kind:    1,
			Content: "explicit https://example.com/image.jpg",
			Tags:    [][]string{{"e", hexID("f"), "", "reply"}},
		},
	}

	embeddings, err := NewHashingEmbeddingProcessor("model-v1", 2, provider).ProcessBatch(context.Background(), events)
	if err != nil {
		t.Fatal(err)
	}
	if got := embeddings[0].Annotation.Embedding; len(got) != 2 || got[0] != 1 {
		t.Fatalf("embedding = %+v, want provider vector", got)
	}

	stance, err := NewStanceProcessor("model-v1", provider).ProcessBatch(context.Background(), events[:1])
	if err != nil {
		t.Fatal(err)
	}
	if stance[0].Annotation.Tags[0].Value != "agree" {
		t.Fatalf("stance = %+v, want provider agree label", stance[0].Annotation.Tags)
	}

	sentiment, err := NewSentimentProcessor("model-v1", provider).ProcessBatch(context.Background(), events[:1])
	if err != nil {
		t.Fatal(err)
	}
	if got := metricValue(sentiment[0].Annotation.Metrics, "sentiment"); got <= 0 {
		t.Fatalf("sentiment = %f, want positive provider score", got)
	}

	nsfw, err := NewNSFWProcessor("model-v1", provider).ProcessBatch(context.Background(), events[1:])
	if err != nil {
		t.Fatal(err)
	}
	if len(nsfw[0].Annotation.Tags) != 1 || nsfw[0].Annotation.Tags[0].Value != "explicit" {
		t.Fatalf("nsfw tags = %+v, want provider explicit tag", nsfw[0].Annotation.Tags)
	}
}

func TestDiscoverHugotModelInventoryReportsAvailableAliases(t *testing.T) {
	root := t.TempDir()
	for _, dir := range []string{"minilm", "roberta-sentiment", "nsfw-image-classifier"} {
		if err := os.Mkdir(filepath.Join(root, dir), 0o755); err != nil {
			t.Fatal(err)
		}
	}

	inventory := DiscoverHugotModelInventory(root)

	if !inventory.RootExists {
		t.Fatalf("root exists = false, want true")
	}
	if got, want := strings.Join(inventory.Available, ","), "embeddings,sentiment,nsfw-image"; got != want {
		t.Fatalf("available = %q, want %q", got, want)
	}
	if got, want := strings.Join(inventory.Missing, ","), "stance,nsfw-text"; got != want {
		t.Fatalf("missing = %q, want %q", got, want)
	}
}

type fakeModelProvider struct {
	embeddings [][]float32
	sentiment  [][]LabelScore
	stance     [][]LabelScore
	nsfwText   [][]LabelScore
	nsfwImages [][]LabelScore
}

func (p *fakeModelProvider) Embed(context.Context, []string) ([][]float32, bool, error) {
	return p.embeddings, len(p.embeddings) > 0, nil
}

func (p *fakeModelProvider) ClassifySentiment(context.Context, []string) ([][]LabelScore, bool, error) {
	return p.sentiment, len(p.sentiment) > 0, nil
}

func (p *fakeModelProvider) ClassifyStance(context.Context, []string, []string) ([][]LabelScore, bool, error) {
	return p.stance, len(p.stance) > 0, nil
}

func (p *fakeModelProvider) ClassifyNSFWText(context.Context, []string) ([][]LabelScore, bool, error) {
	return p.nsfwText, len(p.nsfwText) > 0, nil
}

func (p *fakeModelProvider) ClassifyNSFWImages(context.Context, []string) ([][]LabelScore, bool, error) {
	return p.nsfwImages, len(p.nsfwImages) > 0, nil
}

func (p *fakeModelProvider) Close() error {
	return nil
}

func metricValue(metrics []Metric, name string) float64 {
	for _, metric := range metrics {
		if metric.Name == name {
			return metric.Value
		}
	}
	return 0
}
