import dataclasses
import json
from datetime import datetime, timezone
from pathlib import Path

RESULTS_DIR = Path(__file__).resolve().parent / "results"


def log_benchmark_result(
    label: str, n_papers: int, summary: dict, metrics, report_text: str
) -> dict:
    """Append one structured record to results/benchmark_metrics.jsonl and
    save the full human-readable report to results/{label}_{timestamp}.txt.
    Returns paths written. JSONL is opened in append mode so historical
    runs are never rewritten."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    ts = datetime.now(timezone.utc)
    record = {
        "timestamp": ts.isoformat(),
        "label": label,
        "n_papers": n_papers,
        "summary": summary,
        "metrics": dataclasses.asdict(metrics),
    }

    jsonl_path = RESULTS_DIR / "benchmark_metrics.jsonl"
    # Append mode preserves benchmark history across repeated local runs.
    with open(jsonl_path, "a") as fh:
        fh.write(json.dumps(record, default=str) + "\n")

    stamp = ts.strftime("%Y%m%d_%H%M%S")
    txt_path = RESULTS_DIR / f"{label}_{stamp}.txt"
    txt_path.write_text(report_text)

    return {"jsonl": str(jsonl_path), "txt": str(txt_path)}
