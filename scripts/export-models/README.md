# Export Enrichment Models

Offline helper for preparing Hugot-compatible ONNX model folders for the enrichment worker.

The runtime never downloads models. Export or copy model artifacts outside the deploy path, then mount the resulting directory as the Railway model volume and point `NAGG_ENRICH_MODEL_DIR` at it.

Expected output layout:

```text
models/
  embeddings/
  sentiment/
  stance/
  nsfw-text/
  nsfw-image/
```

Example:

```sh
python3 -m venv .venv
. .venv/bin/activate
pip install -r scripts/export-models/requirements.txt
python scripts/export-models/export_models.py --output-dir /tmp/nagg-models
```

The script uses Hugging Face Optimum's ONNX exporter when a model ID is provided. If a model ID is omitted, that target is skipped so operators can stage only the models they are ready to run.
