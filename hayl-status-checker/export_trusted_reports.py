import datetime
import json
import logging
import os

METRIC_KEYS = (
    "revenue",
    "net_income",
    "eps",
    "operating_profit",
    "ebitda",
    "issued_shares",
    "par_value",
    "market_cap",
    "foreign_percentage",
    "pe_ratio",
    "pb_ratio",
    "dividend_yield",
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPORTS_DIR = os.path.join(BASE_DIR, "data", "raw", "HAYL", "reports")
SOURCE_DIR = os.path.join(REPORTS_DIR, "parsed_opencode")
TRUSTED_DIR = os.path.join(REPORTS_DIR, "parsed_trusted")
SUMMARY_DIR = os.path.join(REPORTS_DIR, "metadata")


def _utc_now_iso():
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def _ensure_dirs():
    os.makedirs(TRUSTED_DIR, exist_ok=True)
    os.makedirs(SUMMARY_DIR, exist_ok=True)


def _null_metric():
    return {"value": None, "unit": None, "page": None, "confidence": None}


def _safe_metric(metric):
    if not isinstance(metric, dict):
        return _null_metric()

    return {
        "value": metric.get("value"),
        "unit": metric.get("unit"),
        "page": metric.get("page"),
        "confidence": metric.get("confidence"),
    }


def _write_json_atomic(file_path, payload):
    temp_path = file_path + ".tmp"
    serialized = json.dumps(payload, ensure_ascii=False, indent=2)
    json.loads(serialized)

    with open(temp_path, "w", encoding="utf-8") as temp_file:
        temp_file.write(serialized)
        temp_file.write("\n")

    with open(temp_path, "r", encoding="utf-8") as verify_file:
        json.load(verify_file)

    os.replace(temp_path, file_path)


def _trusted_record_from_parsed(parsed):
    rejected_metrics = parsed.get("rejected_metrics")
    rejected_names = set()
    if isinstance(rejected_metrics, list):
        for item in rejected_metrics:
            if isinstance(item, dict) and isinstance(item.get("metric"), str):
                rejected_names.add(item["metric"])

    source_metrics = parsed.get("metrics", {})
    trusted_metrics = {}
    trusted_non_null_count = 0

    for key in METRIC_KEYS:
        if key in rejected_names:
            trusted_metrics[key] = _null_metric()
            continue

        metric = _safe_metric(source_metrics.get(key))
        trusted_metrics[key] = metric
        if metric["value"] is not None:
            trusted_non_null_count += 1

    return {
        "symbol": parsed.get("symbol"),
        "report_type": parsed.get("report_type"),
        "financial_year": parsed.get("financial_year"),
        "source_pdf": parsed.get("source_pdf"),
        "trusted_at": _utc_now_iso(),
        "metrics": trusted_metrics,
        "trusted_non_null_metrics": trusted_non_null_count,
    }


def export_trusted_reports():
    _ensure_dirs()
    if not os.path.isdir(SOURCE_DIR):
        return {
            "total_source_reports": 0,
            "trusted_reports_written": 0,
            "dataset_written": False,
        }

    source_files = sorted(
        file_name
        for file_name in os.listdir(SOURCE_DIR)
        if file_name.lower().endswith(".json")
    )

    trusted_records = []
    for file_name in source_files:
        source_path = os.path.join(SOURCE_DIR, file_name)
        with open(source_path, "r", encoding="utf-8") as source_file:
            parsed = json.load(source_file)

        trusted = _trusted_record_from_parsed(parsed)
        trusted_records.append(trusted)

        target_path = os.path.join(TRUSTED_DIR, file_name)
        _write_json_atomic(target_path, trusted)

    dataset_path = os.path.join(TRUSTED_DIR, "trusted_reports_dataset.json")
    _write_json_atomic(dataset_path, trusted_records)

    summary = {
        "run_at": _utc_now_iso(),
        "total_source_reports": len(source_files),
        "trusted_reports_written": len(trusted_records),
        "dataset_written": True,
        "dataset_path": os.path.relpath(dataset_path, BASE_DIR).replace("\\", "/"),
    }

    summary_path = os.path.join(SUMMARY_DIR, f"trusted_export_summary_{summary['run_at'][:10]}.json")
    existing = []
    if os.path.exists(summary_path):
        try:
            with open(summary_path, "r", encoding="utf-8") as source_file:
                loaded = json.load(source_file)
            if isinstance(loaded, list):
                existing = loaded
        except (json.JSONDecodeError, OSError):
            existing = []
    existing.append(summary)
    _write_json_atomic(summary_path, existing)

    return summary


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    summary = export_trusted_reports()
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
