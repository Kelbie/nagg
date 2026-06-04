package enrich

import (
	"context"
	"time"
)

const (
	TaskTopics      = "topics"
	TaskEmbeddings  = "embeddings"
	TaskTrending    = "trending"
	TaskStance      = "stance"
	TaskSentiment   = "sentiment"
	TaskQuality     = "quality"
	TaskControversy = "controversy"
	TaskNSFW        = "nsfw"
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

type LabelScore struct {
	Label string
	Score float64
}

type ModelProvider interface {
	Embed(ctx context.Context, inputs []string) ([][]float32, bool, error)
	ClassifySentiment(ctx context.Context, inputs []string) ([][]LabelScore, bool, error)
	ClassifyStance(ctx context.Context, inputs []string, labels []string) ([][]LabelScore, bool, error)
	ClassifyNSFWText(ctx context.Context, inputs []string) ([][]LabelScore, bool, error)
	ClassifyNSFWImages(ctx context.Context, paths []string) ([][]LabelScore, bool, error)
	Close() error
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
	Embedding    []float32
	Clusters     []TrendingCluster
	ModelVersion string
	ComputedAt   time.Time
}

func (a Annotation) Empty() bool {
	return len(a.Tags) == 0 && len(a.Metrics) == 0 && len(a.Embedding) == 0 && len(a.Clusters) == 0
}

type TrendingCluster struct {
	ID          string
	Window      string
	StartedAt   time.Time
	Category    string
	Subcategory string
	Title       string
	Description string
	Centroid    []float32
	EventCount  uint64
	Score       float64
	ComputedAt  time.Time
}

type ProcessResult struct {
	Annotation Annotation
	Err        error
}

type Processor interface {
	Task() string
	ProcessBatch(ctx context.Context, events []Event) ([]ProcessResult, error)
}

type ClosableProcessor interface {
	Processor
	Close() error
}

type Store interface {
	LoadEnrichmentState(ctx context.Context, task string) (State, bool, error)
	FetchEnrichmentEvents(ctx context.Context, cursor Cursor, limit int) ([]Event, error)
	WriteEnrichmentAnnotations(ctx context.Context, annotations []Annotation) error
	SaveEnrichmentState(ctx context.Context, state State) error
}
