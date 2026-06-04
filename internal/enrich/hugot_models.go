package enrich

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/knights-analytics/hugot"
	"github.com/knights-analytics/hugot/options"
	"github.com/knights-analytics/hugot/pipelines"
)

type HugotModelConfig struct {
	ModelDir        string
	Backend         string
	OnnxLibraryPath string
}

type HugotModelProvider struct {
	config HugotModelConfig

	sessionOnce sync.Once
	session     *hugot.Session
	sessionErr  error

	embeddingOnce sync.Once
	embedding     *pipelines.FeatureExtractionPipeline
	embeddingOK   bool
	embeddingErr  error

	sentimentOnce sync.Once
	sentiment     *pipelines.TextClassificationPipeline
	sentimentOK   bool
	sentimentErr  error

	stanceOnce sync.Once
	stance     *pipelines.ZeroShotClassificationPipeline
	stanceOK   bool
	stanceErr  error

	nsfwTextOnce sync.Once
	nsfwText     *pipelines.TextClassificationPipeline
	nsfwTextOK   bool
	nsfwTextErr  error

	nsfwImageOnce sync.Once
	nsfwImage     *pipelines.ImageClassificationPipeline
	nsfwImageOK   bool
	nsfwImageErr  error

	closeOnce sync.Once
	closeErr  error
}

type HugotModelInventory struct {
	Root       string
	RootExists bool
	Available  []string
	Missing    []string
}

type hugotModelSpec struct {
	key     string
	aliases []string
}

var hugotModelSpecs = []hugotModelSpec{
	{key: "embeddings", aliases: []string{"embeddings", "embedding", "feature-extraction", "minilm"}},
	{key: "sentiment", aliases: []string{"sentiment", "twitter-roberta-sentiment", "roberta-sentiment"}},
	{key: "stance", aliases: []string{"stance", "nli", "zero-shot-stance", "zero-shot"}},
	{key: "nsfw-text", aliases: []string{"nsfw-text", "nsfw", "nsfw-text-classifier"}},
	{key: "nsfw-image", aliases: []string{"nsfw-image", "nsfw-image-classifier"}},
}

func NewHugotModelProvider(config HugotModelConfig) *HugotModelProvider {
	config.ModelDir = strings.TrimSpace(config.ModelDir)
	config.Backend = strings.ToLower(strings.TrimSpace(config.Backend))
	config.OnnxLibraryPath = strings.TrimSpace(config.OnnxLibraryPath)
	return &HugotModelProvider{config: config}
}

func DiscoverHugotModelInventory(modelDir string) HugotModelInventory {
	root := strings.TrimSpace(modelDir)
	inventory := HugotModelInventory{
		Root:      root,
		Available: []string{},
		Missing:   []string{},
	}
	if root == "" {
		for _, spec := range hugotModelSpecs {
			inventory.Missing = append(inventory.Missing, spec.key)
		}
		return inventory
	}
	info, err := os.Stat(root)
	if err != nil || !info.IsDir() {
		for _, spec := range hugotModelSpecs {
			inventory.Missing = append(inventory.Missing, spec.key)
		}
		return inventory
	}
	inventory.RootExists = true
	for _, spec := range hugotModelSpecs {
		if _, ok := findModelPath(root, spec.aliases...); ok {
			inventory.Available = append(inventory.Available, spec.key)
			continue
		}
		inventory.Missing = append(inventory.Missing, spec.key)
	}
	return inventory
}

func (p *HugotModelProvider) Embed(ctx context.Context, inputs []string) ([][]float32, bool, error) {
	pipeline, ok, err := p.embeddingPipeline(ctx)
	if err != nil || !ok {
		return nil, ok, err
	}
	output, err := pipeline.RunPipeline(ctx, inputs)
	if err != nil {
		return nil, true, err
	}
	if len(output.Embeddings) != len(inputs) {
		return nil, true, fmt.Errorf("hugot embeddings returned %d outputs for %d inputs", len(output.Embeddings), len(inputs))
	}
	return output.Embeddings, true, nil
}

func (p *HugotModelProvider) ClassifySentiment(ctx context.Context, inputs []string) ([][]LabelScore, bool, error) {
	pipeline, ok, err := p.sentimentPipeline(ctx)
	if err != nil || !ok {
		return nil, ok, err
	}
	return runTextClassification(ctx, pipeline, inputs)
}

func (p *HugotModelProvider) ClassifyStance(ctx context.Context, inputs []string, labels []string) ([][]LabelScore, bool, error) {
	pipeline, ok, err := p.stancePipeline(ctx, labels)
	if err != nil || !ok {
		return nil, ok, err
	}
	output, err := pipeline.RunPipeline(ctx, inputs)
	if err != nil {
		return nil, true, err
	}
	if len(output.ClassificationOutputs) != len(inputs) {
		return nil, true, fmt.Errorf("hugot stance returned %d outputs for %d inputs", len(output.ClassificationOutputs), len(inputs))
	}
	out := make([][]LabelScore, len(output.ClassificationOutputs))
	for i, result := range output.ClassificationOutputs {
		out[i] = make([]LabelScore, 0, len(result.SortedValues))
		for _, value := range result.SortedValues {
			out[i] = append(out[i], LabelScore{Label: value.Key, Score: value.Value})
		}
	}
	return out, true, nil
}

func (p *HugotModelProvider) ClassifyNSFWText(ctx context.Context, inputs []string) ([][]LabelScore, bool, error) {
	pipeline, ok, err := p.nsfwTextPipeline(ctx)
	if err != nil || !ok {
		return nil, ok, err
	}
	return runTextClassification(ctx, pipeline, inputs)
}

func (p *HugotModelProvider) ClassifyNSFWImages(ctx context.Context, paths []string) ([][]LabelScore, bool, error) {
	pipeline, ok, err := p.nsfwImagePipeline(ctx)
	if err != nil || !ok {
		return nil, ok, err
	}
	output, err := pipeline.RunPipeline(ctx, paths)
	if err != nil {
		return nil, true, err
	}
	if len(output.Predictions) != len(paths) {
		return nil, true, fmt.Errorf("hugot nsfw image returned %d outputs for %d inputs", len(output.Predictions), len(paths))
	}
	out := make([][]LabelScore, len(output.Predictions))
	for i, predictions := range output.Predictions {
		out[i] = make([]LabelScore, 0, len(predictions))
		for _, prediction := range predictions {
			out[i] = append(out[i], LabelScore{Label: prediction.Label, Score: float64(prediction.Score)})
		}
	}
	return out, true, nil
}

func (p *HugotModelProvider) Close() error {
	if p == nil || p.session == nil {
		return nil
	}
	p.closeOnce.Do(func() {
		p.closeErr = p.session.Destroy()
	})
	return p.closeErr
}

func (p *HugotModelProvider) sessionFor(ctx context.Context) (*hugot.Session, error) {
	p.sessionOnce.Do(func() {
		switch p.config.Backend {
		case "ort", "onnx", "onnxruntime":
			opts := []options.WithOption{}
			if p.config.OnnxLibraryPath != "" {
				opts = append(opts, options.WithOnnxLibraryPath(p.config.OnnxLibraryPath))
			}
			p.session, p.sessionErr = hugot.NewORTSession(ctx, opts...)
		case "", "go", "gomlx":
			p.session, p.sessionErr = hugot.NewGoSession(ctx)
		default:
			p.sessionErr = fmt.Errorf("unsupported hugot backend %q", p.config.Backend)
		}
	})
	return p.session, p.sessionErr
}

func (p *HugotModelProvider) embeddingPipeline(ctx context.Context) (*pipelines.FeatureExtractionPipeline, bool, error) {
	p.embeddingOnce.Do(func() {
		path, ok := p.modelPath("embeddings")
		if !ok {
			return
		}
		session, err := p.sessionFor(ctx)
		if err != nil {
			p.embeddingErr = err
			return
		}
		p.embedding, p.embeddingErr = hugot.NewPipeline(session, hugot.FeatureExtractionConfig{
			ModelPath: path,
			Name:      "nagg-embeddings",
			Options:   []hugot.FeatureExtractionOption{pipelines.WithNormalization()},
		})
		p.embeddingOK = p.embeddingErr == nil
	})
	return p.embedding, p.embeddingOK, p.embeddingErr
}

func (p *HugotModelProvider) sentimentPipeline(ctx context.Context) (*pipelines.TextClassificationPipeline, bool, error) {
	p.sentimentOnce.Do(func() {
		path, ok := p.modelPath("sentiment")
		if !ok {
			return
		}
		session, err := p.sessionFor(ctx)
		if err != nil {
			p.sentimentErr = err
			return
		}
		p.sentiment, p.sentimentErr = hugot.NewPipeline(session, hugot.TextClassificationConfig{
			ModelPath: path,
			Name:      "nagg-sentiment",
			Options:   []hugot.TextClassificationOption{pipelines.WithSoftmax()},
		})
		p.sentimentOK = p.sentimentErr == nil
	})
	return p.sentiment, p.sentimentOK, p.sentimentErr
}

func (p *HugotModelProvider) stancePipeline(ctx context.Context, labels []string) (*pipelines.ZeroShotClassificationPipeline, bool, error) {
	p.stanceOnce.Do(func() {
		path, ok := p.modelPath("stance")
		if !ok {
			return
		}
		session, err := p.sessionFor(ctx)
		if err != nil {
			p.stanceErr = err
			return
		}
		p.stance, p.stanceErr = hugot.NewPipeline(session, hugot.ZeroShotClassificationConfig{
			ModelPath: path,
			Name:      "nagg-stance",
			Options: []hugot.ZeroShotClassificationOption{
				pipelines.WithLabels(labels),
				pipelines.WithHypothesisTemplate("The stance of this reply is {}."),
				pipelines.WithMultilabel(false),
			},
		})
		p.stanceOK = p.stanceErr == nil
	})
	return p.stance, p.stanceOK, p.stanceErr
}

func (p *HugotModelProvider) nsfwTextPipeline(ctx context.Context) (*pipelines.TextClassificationPipeline, bool, error) {
	p.nsfwTextOnce.Do(func() {
		path, ok := p.modelPath("nsfw-text")
		if !ok {
			return
		}
		session, err := p.sessionFor(ctx)
		if err != nil {
			p.nsfwTextErr = err
			return
		}
		p.nsfwText, p.nsfwTextErr = hugot.NewPipeline(session, hugot.TextClassificationConfig{
			ModelPath: path,
			Name:      "nagg-nsfw-text",
			Options:   []hugot.TextClassificationOption{pipelines.WithSoftmax()},
		})
		p.nsfwTextOK = p.nsfwTextErr == nil
	})
	return p.nsfwText, p.nsfwTextOK, p.nsfwTextErr
}

func (p *HugotModelProvider) nsfwImagePipeline(ctx context.Context) (*pipelines.ImageClassificationPipeline, bool, error) {
	p.nsfwImageOnce.Do(func() {
		path, ok := p.modelPath("nsfw-image")
		if !ok {
			return
		}
		session, err := p.sessionFor(ctx)
		if err != nil {
			p.nsfwImageErr = err
			return
		}
		p.nsfwImage, p.nsfwImageErr = hugot.NewPipeline(session, hugot.ImageClassificationConfig{
			ModelPath: path,
			Name:      "nagg-nsfw-image",
			Options:   []hugot.ImageClassificationOption{pipelines.WithTopK(5)},
		})
		p.nsfwImageOK = p.nsfwImageErr == nil
	})
	return p.nsfwImage, p.nsfwImageOK, p.nsfwImageErr
}

func (p *HugotModelProvider) modelPath(names ...string) (string, bool) {
	root := strings.TrimSpace(p.config.ModelDir)
	if root == "" {
		return "", false
	}
	for _, name := range names {
		for _, spec := range hugotModelSpecs {
			if spec.key != name {
				continue
			}
			return findModelPath(root, spec.aliases...)
		}
	}
	return findModelPath(root, names...)
}

func findModelPath(root string, names ...string) (string, bool) {
	for _, name := range names {
		path := filepath.Join(root, name)
		info, err := os.Stat(path)
		if err == nil && info.IsDir() {
			return path, true
		}
	}
	return "", false
}

func runTextClassification(
	ctx context.Context,
	pipeline *pipelines.TextClassificationPipeline,
	inputs []string,
) ([][]LabelScore, bool, error) {
	output, err := pipeline.RunPipeline(ctx, inputs)
	if err != nil {
		return nil, true, err
	}
	if len(output.ClassificationOutputs) != len(inputs) {
		return nil, true, fmt.Errorf("hugot text classification returned %d outputs for %d inputs", len(output.ClassificationOutputs), len(inputs))
	}
	out := make([][]LabelScore, len(output.ClassificationOutputs))
	for i, result := range output.ClassificationOutputs {
		out[i] = make([]LabelScore, 0, len(result))
		for _, value := range result {
			out[i] = append(out[i], LabelScore{Label: value.Label, Score: float64(value.Score)})
		}
	}
	return out, true, nil
}
