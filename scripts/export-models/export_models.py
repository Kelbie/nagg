#!/usr/bin/env python3
"""Export Hugging Face models to Hugot-compatible ONNX folders.

This is an offline operator tool. It is intentionally not imported by the Go
runtime or used in the Docker build.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


DEFAULT_MODELS = {
    "embeddings": ("sentence-transformers/all-MiniLM-L6-v2", "feature-extraction"),
    "sentiment": ("cardiffnlp/twitter-roberta-base-sentiment-latest", "text-classification"),
    "stance": ("MoritzLaurer/deberta-v3-base-zeroshot-v1", "zero-shot-classification"),
    "nsfw-text": ("KoalaAI/Text-Moderation", "text-classification"),
    "nsfw-image": ("Falconsai/nsfw_image_detection", "image-classification"),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--opset", default="17")
    for key, (model_id, _) in DEFAULT_MODELS.items():
        parser.add_argument(
            f"--{key}-model",
            default=model_id,
            help=f"Hugging Face model id for {key}; pass an empty string to skip.",
        )
    return parser.parse_args()


def export_model(model_id: str, task: str, destination: Path, opset: str) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable,
        "-m",
        "optimum.exporters.onnx",
        "--model",
        model_id,
        "--task",
        task,
        "--opset",
        opset,
        str(destination),
    ]
    subprocess.run(command, check=True)


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    for key, (_, task) in DEFAULT_MODELS.items():
        model_id = getattr(args, f"{key.replace('-', '_')}_model").strip()
        if not model_id:
            print(f"skip {key}")
            continue
        destination = args.output_dir / key
        print(f"export {key}: {model_id} -> {destination}")
        export_model(model_id, task, destination, args.opset)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
