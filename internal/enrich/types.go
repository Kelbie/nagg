package enrich

import (
	"context"
	"time"
)

const (
	TaskQuality = "quality"
)

type Cursor struct {
	CreatedAt time.Time
	EventID   string
}

type State struct {
	Task      string
	Cursor    Cursor
	Processed uint64
	Failed    uint64
	UpdatedAt time.Time
}

type Event struct {
	ID        string
	PubKey    string
	Kind      int
	CreatedAt time.Time
	Content   string
	Tags      [][]string
}

type Tag struct {
	Key   string
	Value string
	Extra []string
}

type Metric struct {
	Name  string
	Value float64
}

type Annotation struct {
	Event        Event
	Tags         []Tag
	Metrics      []Metric
	ModelVersion string
	ComputedAt   time.Time
}

func (a Annotation) Empty() bool {
	return len(a.Tags) == 0 && len(a.Metrics) == 0
}

type ProcessResult struct {
	Annotation Annotation
	Err        error
}

type Processor interface {
	Task() string
	ProcessBatch(ctx context.Context, events []Event) ([]ProcessResult, error)
}

type Store interface {
	LoadEnrichmentState(ctx context.Context, task string) (State, bool, error)
	FetchEnrichmentEvents(ctx context.Context, cursor Cursor, limit int) ([]Event, error)
	WriteEnrichmentAnnotations(ctx context.Context, annotations []Annotation) error
	SaveEnrichmentState(ctx context.Context, state State) error
}
