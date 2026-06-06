package enrich

import (
	"context"
	"testing"
)

func TestNewProcessorsBuildsQualityProcessor(t *testing.T) {
	processors, err := NewProcessors([]string{TaskQuality}, ProcessorConfig{ModelVersion: "test-v1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(processors) != 1 {
		t.Fatalf("processors = %+v, want one processor", processors)
	}
	if processors[0].Task() != TaskQuality {
		t.Fatalf("processors[0].Task() = %q, want %q", processors[0].Task(), TaskQuality)
	}
}

func TestNewProcessorsRejectsUnsupportedTask(t *testing.T) {
	if _, err := NewProcessors([]string{"embeddings"}, ProcessorConfig{ModelVersion: "test-v1"}); err == nil {
		t.Fatal("expected error for unsupported task")
	}
}

func TestContributionQualityProcessorScoresReplies(t *testing.T) {
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
	if !quality[1].Annotation.Empty() {
		t.Fatalf("top-level quality annotation = %+v, want empty", quality[1].Annotation)
	}
}

func metricValue(metrics []Metric, name string) float64 {
	for _, metric := range metrics {
		if metric.Name == name {
			return metric.Value
		}
	}
	return 0
}
