import argparse
import datetime
import json
import logging
import os
import re
import shutil
import subprocess

import pdfplumber
from export_trusted_reports import export_trusted_reports

SYMBOL = "HAYL.N0000"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPORTS_DIR = os.path.join(BASE_DIR, "data", "raw", "HAYL", "reports")
PDF_DIR = os.path.join(REPORTS_DIR, "pdfs")
PARSED_DIR = os.path.join(REPORTS_DIR, "parsed_opencode")
BASELINE_PARSED_DIR = os.path.join(REPORTS_DIR, "parsed")
METADATA_DIR = os.path.join(REPORTS_DIR, "metadata")
ANALYZER_INPUT_DIR = os.path.join(REPORTS_DIR, "analyzer_input")
OPENCODE_TIMEOUT_SECONDS = 420
MAX_OPENCODE_RETRIES = 2
ANSI_PATTERN = re.compile(r"\x1b\[[0-9;]*m")
MAX_ANALYZER_CHARS = 70000
MAX_PAGE_BLOCK_CHARS = 3000
ALWAYS_INCLUDE_FIRST_PAGES = 8
MAX_TABLES_PER_PAGE = 6
MAX_ROWS_PER_TABLE = 30

KEYWORD_HINTS = (
    "revenue",
    "turnover",
    "net income",
    "profit after tax",
    "earnings per share",
    "eps",
    "operating",
    "ebitda",
    "market capitalisation",
    "market capitalization",
    "p/e",
    "p/b",
    "dividend yield",
)

METRIC_HINTS = {
    "revenue": ("revenue", "turnover", "sales"),
    "net_income": ("net income", "profit after tax", "pat", "profit for the year"),
    "eps": ("eps", "earnings per share"),
    "operating_profit": ("operating profit", "operating income", "results from operating"),
    "ebitda": ("ebitda",),
    "issued_shares": ("issued shares", "no. of shares", "shares"),
    "par_value": ("par value",),
    "market_cap": ("market capitalisation", "market capitalization", "market cap"),
    "foreign_percentage": ("foreign percentage", "foreign"),
    "pe_ratio": ("p/e", "price earnings", "pe ratio"),
    "pb_ratio": ("p/b", "price book", "pb ratio"),
    "dividend_yield": ("dividend yield",),
}

NUMBER_PATTERN = re.compile(r"\(?-?\d[\d,]*(?:\.\d+)?\)?")

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

AMOUNT_METRICS = {
    "revenue",
    "net_income",
    "operating_profit",
    "ebitda",
    "market_cap",
}


def _utc_now_iso():
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def _ensure_output_dirs():
    os.makedirs(PARSED_DIR, exist_ok=True)
    os.makedirs(METADATA_DIR, exist_ok=True)
    os.makedirs(ANALYZER_INPUT_DIR, exist_ok=True)


def _slugify(value):
    cleaned = re.sub(r"[^A-Za-z0-9]+", "_", value.strip().lower())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned or "report"


def _financial_year_from_name(file_name):
    match = re.search(r"fy[_\-\s]*([0-9]{2,4})[_\-/\s]*([0-9]{2,4})", file_name, flags=re.IGNORECASE)
    if match:
        return f"FY{match.group(1)}-{match.group(2)}"

    match = re.search(r"((?:19|20)[0-9]{2})[_\-/\s]*([0-9]{2})", file_name)
    if match:
        return f"FY{match.group(1)}-{match.group(2)}"

    match = re.search(r"(?<![0-9])((?:19|20)[0-9]{2})(?![0-9])", file_name)
    if match:
        return f"FY{match.group(1)}"

    return None


def _report_type_from_name(file_name):
    lowered = file_name.lower()
    if "quarterly" in lowered or re.search(r"\bq[1-4]\b", lowered):
        return "quarterly_results"
    if lowered.startswith("group_") or "group" in lowered:
        return "group_company_reports"
    return "annual_reports"


def _build_prompt(file_name, report_type, financial_year):
    return (
        "You are a financial report extractor. "
        "Analyze the attached text file, which contains OCR/text extracted from a financial PDF, "
        "and return ONLY valid JSON with no markdown and no extra text. "
        "Context: symbol="
        f"{SYMBOL}, report_type={report_type}, financial_year={financial_year or 'null'}, file_name={file_name}. "
        "JSON shape must be exactly: "
        "{"
        "\"metrics\":{"
        "\"revenue\":{\"value\":number|null,\"unit\":string|null,\"page\":number|null,\"confidence\":number|null},"
        "\"net_income\":{\"value\":number|null,\"unit\":string|null,\"page\":number|null,\"confidence\":number|null},"
        "\"eps\":{\"value\":number|null,\"unit\":string|null,\"page\":number|null,\"confidence\":number|null},"
        "\"operating_profit\":{\"value\":number|null,\"unit\":string|null,\"page\":number|null,\"confidence\":number|null},"
        "\"ebitda\":{\"value\":number|null,\"unit\":string|null,\"page\":number|null,\"confidence\":number|null},"
        "\"issued_shares\":{\"value\":number|null,\"unit\":string|null,\"page\":number|null,\"confidence\":number|null},"
        "\"par_value\":{\"value\":number|null,\"unit\":string|null,\"page\":number|null,\"confidence\":number|null},"
        "\"market_cap\":{\"value\":number|null,\"unit\":string|null,\"page\":number|null,\"confidence\":number|null},"
        "\"foreign_percentage\":{\"value\":number|null,\"unit\":string|null,\"page\":number|null,\"confidence\":number|null},"
        "\"pe_ratio\":{\"value\":number|null,\"unit\":string|null,\"page\":number|null,\"confidence\":number|null},"
        "\"pb_ratio\":{\"value\":number|null,\"unit\":string|null,\"page\":number|null,\"confidence\":number|null},"
        "\"dividend_yield\":{\"value\":number|null,\"unit\":string|null,\"page\":number|null,\"confidence\":number|null}"
        "}"
        "}. "
        "Rules: "
        "use decimal numbers only (no commas), null if unknown, page as integer if known."
    )


def _strip_ansi(text):
    return ANSI_PATTERN.sub("", text or "")


def _extract_json_from_output(raw_output):
    cleaned = _strip_ansi(raw_output).strip()
    fence_match = re.search(r"```(?:json)?\s*([\s\S]*?)```", cleaned, flags=re.IGNORECASE)
    if fence_match:
        cleaned = fence_match.group(1).strip()

    decoder = json.JSONDecoder()
    for idx, char in enumerate(cleaned):
        if char != "{":
            continue
        try:
            parsed, _ = decoder.raw_decode(cleaned[idx:])
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            continue
    return None


def _to_number(value):
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().replace(",", "")
        if cleaned == "":
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _to_int(value):
    numeric = _to_number(value)
    if numeric is None:
        return None
    return int(numeric)


def _canonical_unit(metric_name, unit_value):
    if unit_value is None:
        return None

    text = str(unit_value).strip().lower()
    compact = re.sub(r"[\s\.\-_/]", "", text)

    if metric_name == "issued_shares":
        if "mn" in compact or "million" in compact:
            return "shares_mn"
        if "share" in compact or compact.startswith("no"):
            return "shares"
        return "shares" if compact else None

    if metric_name in ("pe_ratio", "pb_ratio"):
        return "ratio"

    if metric_name == "dividend_yield":
        if "%" in text or "percent" in text:
            return "percent"
        return "percent" if text else None

    if metric_name == "foreign_percentage":
        if "%" in text or "percent" in text:
            return "percent"
        return "percent" if text else None

    if metric_name == "eps":
        return "lkr_per_share"

    if metric_name in AMOUNT_METRICS or metric_name == "par_value":
        if "bn" in compact:
            return "amount_bn"
        if "mn" in compact or "million" in compact or "mrs" in compact:
            return "amount_mn"
        if "000" in compact:
            return "amount_thousand"
        if "rs" in compact or "lkr" in compact:
            return "amount_lkr"
        return "amount_lkr" if compact else None

    return text or None


def _normalize_value_for_unit(metric_name, value, unit):
    if value is None or unit is None:
        return value, unit

    value = float(value)
    if metric_name in AMOUNT_METRICS or metric_name == "par_value":
        if unit == "amount_bn":
            return value * 1000.0, "lkr_mn"
        if unit == "amount_thousand":
            return value / 1000.0, "lkr_mn"
        if unit == "amount_lkr":
            return value / 1_000_000.0, "lkr_mn"
        if unit == "amount_mn":
            return value, "lkr_mn"
        return value, "lkr_mn"

    if metric_name == "issued_shares":
        if unit == "shares_mn":
            return value * 1_000_000.0, "shares"
        return value, "shares"

    return value, unit


def _normalize_metric(metric_name, metric):
    if not isinstance(metric, dict):
        metric = {}
    raw_value = _to_number(metric.get("value"))
    raw_unit = metric.get("unit") if isinstance(metric.get("unit"), str) else None
    canonical_unit = _canonical_unit(metric_name, raw_unit)
    normalized_value, normalized_unit = _normalize_value_for_unit(metric_name, raw_value, canonical_unit)
    return {
        "value": normalized_value,
        "unit": normalized_unit,
        "page": _to_int(metric.get("page")),
        "confidence": _to_number(metric.get("confidence")),
    }


def _normalize_parsed_payload(payload):
    metrics = payload.get("metrics", {}) if isinstance(payload, dict) else {}
    normalized_metrics = {}
    for key in METRIC_KEYS:
        normalized_metrics[key] = _normalize_metric(key, metrics.get(key))
    return {"metrics": normalized_metrics}


def _all_metrics_null(metrics):
    for key in METRIC_KEYS:
        metric = metrics.get(key) if isinstance(metrics.get(key), dict) else {}
        if metric.get("value") is not None:
            return False
    return True


def _load_baseline_metrics(stem):
    baseline_path = os.path.join(BASELINE_PARSED_DIR, f"{stem}.json")
    if not os.path.exists(baseline_path):
        return None
    try:
        with open(baseline_path, "r", encoding="utf-8") as source:
            loaded = json.load(source)
        metrics = loaded.get("metrics", {})
        return metrics if isinstance(metrics, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def _apply_baseline_fallback(normalized_metrics, stem):
    if not _all_metrics_null(normalized_metrics):
        return normalized_metrics, False

    baseline_metrics = _load_baseline_metrics(stem)
    if not baseline_metrics:
        return normalized_metrics, False

    merged = dict(normalized_metrics)
    changed = False
    for key in METRIC_KEYS:
        current = merged.get(key) if isinstance(merged.get(key), dict) else {}
        if current.get("value") is not None:
            continue
        baseline_metric = baseline_metrics.get(key)
        normalized_baseline = _normalize_metric(key, baseline_metric if isinstance(baseline_metric, dict) else {})
        if normalized_baseline.get("value") is None:
            continue
        merged[key] = normalized_baseline
        changed = True

    return merged, changed


def _extract_line_numbers(text, metric):
    hints = METRIC_HINTS.get(metric, ())
    if not hints:
        return []

    lines = text.splitlines()
    matches = []
    for idx, line in enumerate(lines, start=1):
        lowered = line.lower()
        if any(hint in lowered for hint in hints):
            matches.append((idx, line))
    return matches


def _parse_number_token(token):
    cleaned = token.strip().replace(",", "")
    if not cleaned:
        return None
    negative = cleaned.startswith("(") and cleaned.endswith(")")
    cleaned = cleaned.replace("(", "").replace(")", "")
    try:
        parsed = float(cleaned)
        return -parsed if negative else parsed
    except ValueError:
        return None


def _scaled_candidates(value):
    if value is None:
        return []
    value = float(value)
    scaled = [value, value * 1000.0, value / 1000.0, value * 1_000_000.0, value / 1_000_000.0]
    dedup = []
    seen = set()
    for item in scaled:
        rounded = round(item, 6)
        if rounded in seen:
            continue
        seen.add(rounded)
        dedup.append(item)
    return dedup


def _line_has_close_number(line, value):
    candidates = _scaled_candidates(value)
    if not candidates:
        return False
    numbers = []
    for token in NUMBER_PATTERN.findall(line):
        parsed = _parse_number_token(token)
        if parsed is not None:
            numbers.append(parsed)

    for found in numbers:
        for candidate in candidates:
            tolerance = max(0.05, abs(candidate) * 0.015)
            if abs(found - candidate) <= tolerance:
                return True
    return False


def _string_candidates(value):
    candidates = set()
    for candidate in _scaled_candidates(value):
        as_int = int(round(candidate))
        candidates.add(str(as_int))
        candidates.add(f"{candidate:.2f}".rstrip("0").rstrip("."))
        candidates.add(f"{as_int:,}")
        candidates.add(f"{candidate:,.2f}".rstrip("0").rstrip("."))
    return {item for item in candidates if item}


def _is_low_quality_text(text):
    if not text:
        return True
    ascii_count = sum(1 for char in text if ord(char) < 128)
    ascii_ratio = ascii_count / len(text)
    cid_count = text.lower().count("(cid:")
    return ascii_ratio < 0.75 or cid_count > 200


def _sanitize_metrics_with_text(metrics, analyzer_text):
    sanitized = {}
    rejected = []
    low_quality = _is_low_quality_text(analyzer_text)

    for metric_name in METRIC_KEYS:
        metric = metrics.get(metric_name) if isinstance(metrics.get(metric_name), dict) else {}
        entry = dict(metric)
        value = entry.get("value")
        if value is None:
            sanitized[metric_name] = entry
            continue

        reasons = []

        if metric_name in AMOUNT_METRICS and abs(float(value)) < 1.0:
            reasons.append("implausibly_small_for_lkr_mn")

        lines = _extract_line_numbers(analyzer_text, metric_name)
        keyword_match = any(_line_has_close_number(line, value) for _, line in lines)
        global_match = any(candidate in analyzer_text for candidate in _string_candidates(value))

        if not (keyword_match or global_match):
            reasons.append("value_not_found_in_source_text")

        if reasons:
            rejected.append(
                {
                    "metric": metric_name,
                    "value": value,
                    "unit": entry.get("unit"),
                    "reasons": reasons,
                    "low_quality_text": low_quality,
                }
            )
            entry = {"value": None, "unit": None, "page": None, "confidence": None}

        sanitized[metric_name] = entry

    return sanitized, rejected, low_quality


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


def _is_up_to_date(pdf_path, output_path):
    if not os.path.exists(output_path):
        return False
    return os.path.getmtime(output_path) >= os.path.getmtime(pdf_path)


def _summary_file_path(run_at):
    return os.path.join(METADATA_DIR, f"opencode_extract_summary_{run_at[:10]}.json")


def _append_summary_record(summary):
    run_record = {
        "run_at": _utc_now_iso(),
        "symbol": SYMBOL,
        "analyzer": "opencode",
        "total_pdfs": summary["total_pdfs"],
        "processed": summary["processed"],
        "skipped": summary["skipped"],
        "failed": summary["failed"],
    }
    file_path = _summary_file_path(run_record["run_at"])
    records = []

    if os.path.exists(file_path):
        try:
            with open(file_path, "r", encoding="utf-8") as source_file:
                loaded = json.load(source_file)
            if isinstance(loaded, list):
                records = loaded
        except (json.JSONDecodeError, OSError):
            records = []

    records.append(run_record)
    _write_json_atomic(file_path, records)


def _extract_pdf_text_for_analyzer(pdf_path):
    page_blocks = []
    current_chars = 0

    with pdfplumber.open(pdf_path) as pdf:
        page_count = len(pdf.pages)
        for page_index, page in enumerate(pdf.pages, start=1):
            page_lines = [f"[Page {page_index}]"]
            text = page.extract_text() or ""
            if text.strip():
                page_lines.append(text.strip())

            try:
                tables = page.extract_tables() or []
            except Exception:
                tables = []

            for table in tables[:MAX_TABLES_PER_PAGE]:
                page_lines.append("[Table]")
                for row in table[:MAX_ROWS_PER_TABLE]:
                    if not isinstance(row, list):
                        continue
                    row_values = [str(cell).strip() for cell in row if cell not in (None, "")]
                    if row_values:
                        page_lines.append(" | ".join(row_values))

            page_block = "\n".join(page_lines).strip()
            if not page_block:
                continue

            if len(page_block) > MAX_PAGE_BLOCK_CHARS:
                page_block = page_block[:MAX_PAGE_BLOCK_CHARS]

            lowered = page_block.lower()
            keyword_hits = sum(1 for hint in KEYWORD_HINTS if hint in lowered)
            numeric_hits = len(re.findall(r"\d", page_block))
            cid_hits = lowered.count("(cid:")
            score = keyword_hits * 10 + min(numeric_hits // 30, 8)
            if page_index <= ALWAYS_INCLUDE_FIRST_PAGES:
                score += 3
            if cid_hits > 20:
                score -= 8

            page_blocks.append(
                {
                    "page": page_index,
                    "block": page_block,
                    "score": score,
                    "keyword_hits": keyword_hits,
                }
            )

    if not page_blocks:
        return {"text": "", "page_count": 0, "char_count": 0}

    selected = []
    selected_pages = set()

    for block in page_blocks:
        if block["page"] <= ALWAYS_INCLUDE_FIRST_PAGES:
            selected.append(block)
            selected_pages.add(block["page"])

    scored = sorted(page_blocks, key=lambda item: (item["score"], item["keyword_hits"], -item["page"]), reverse=True)
    for block in scored:
        if block["page"] in selected_pages:
            continue
        projected = current_chars + len(block["block"])
        if projected > MAX_ANALYZER_CHARS:
            continue
        selected.append(block)
        selected_pages.add(block["page"])
        current_chars = projected
        if current_chars >= MAX_ANALYZER_CHARS:
            break

    selected = sorted(selected, key=lambda item: item["page"])
    chunks = []
    current_chars = 0
    for block in selected:
        remaining = MAX_ANALYZER_CHARS - current_chars
        if remaining <= 0:
            break
        text_block = block["block"][:remaining]
        chunks.append(text_block)
        current_chars += len(text_block)

    return {
        "text": "\n\n".join(chunks),
        "page_count": page_count,
        "char_count": current_chars,
    }


def _analyzer_input_file_for_pdf(pdf_path):
    stem = _slugify(os.path.splitext(os.path.basename(pdf_path))[0])
    return os.path.join(ANALYZER_INPUT_DIR, f"{stem}.txt")


def _prepare_analyzer_input(pdf_path):
    input_path = _analyzer_input_file_for_pdf(pdf_path)
    if os.path.exists(input_path) and os.path.getmtime(input_path) >= os.path.getmtime(pdf_path):
        try:
            with open(input_path, "r", encoding="utf-8") as source:
                text = source.read()
            return {"input_path": input_path, "page_count": None, "char_count": len(text), "text": text}
        except OSError:
            pass

    extracted = _extract_pdf_text_for_analyzer(pdf_path)
    text = extracted["text"]
    if not text.strip():
        return None

    temp_path = input_path + ".tmp"
    with open(temp_path, "w", encoding="utf-8") as target:
        target.write(text)
        target.write("\n")
    os.replace(temp_path, input_path)

    return {
        "input_path": input_path,
        "page_count": extracted["page_count"],
        "char_count": extracted["char_count"],
        "text": text,
    }


def _run_opencode_for_text(input_path, file_name, report_type, financial_year):
    executable = shutil.which("opencode.cmd") or shutil.which("opencode")
    if not executable:
        logging.error("opencode executable not found in PATH.")
        return None

    prompt = _build_prompt(file_name=file_name, report_type=report_type, financial_year=financial_year)
    command = [
        executable,
        "run",
        prompt,
        "--file",
        input_path,
        "--dir",
        BASE_DIR,
    ]

    for attempt in range(1, MAX_OPENCODE_RETRIES + 1):
        try:
            result = subprocess.run(
                command,
                cwd=BASE_DIR,
                capture_output=True,
                text=True,
                timeout=OPENCODE_TIMEOUT_SECONDS,
                check=False,
            )
            if result.returncode != 0:
                logging.error(
                    "opencode failed for %s (attempt %s/%s): %s",
                    input_path,
                    attempt,
                    MAX_OPENCODE_RETRIES,
                    (result.stderr or "").strip(),
                )
                continue

            payload = _extract_json_from_output(result.stdout)
            if payload is None:
                output_preview = _strip_ansi(result.stdout or "").strip().replace("\n", " ")
                logging.error("Could not parse JSON from opencode output for %s: %s", input_path, output_preview[:300])
                continue
            return payload
        except subprocess.TimeoutExpired:
            logging.error("opencode timeout for %s (attempt %s/%s)", input_path, attempt, MAX_OPENCODE_RETRIES)

    return None


def extract_with_opencode(max_files=None, force=False, match=None):
    _ensure_output_dirs()
    if not os.path.isdir(PDF_DIR):
        return {"total_pdfs": 0, "processed": 0, "skipped": 0, "failed": 0}

    pdf_files = sorted(
        os.path.join(PDF_DIR, name)
        for name in os.listdir(PDF_DIR)
        if name.lower().endswith(".pdf")
    )
    if match:
        needle = match.lower()
        pdf_files = [path for path in pdf_files if needle in os.path.basename(path).lower()]
    if max_files is not None:
        pdf_files = pdf_files[: max_files if max_files > 0 else 0]

    processed = 0
    skipped = 0
    failed = 0

    for pdf_path in pdf_files:
        stem = _slugify(os.path.splitext(os.path.basename(pdf_path))[0])
        output_path = os.path.join(PARSED_DIR, f"{stem}.json")
        if not force and _is_up_to_date(pdf_path, output_path):
            skipped += 1
            continue

        file_name = os.path.basename(pdf_path)
        report_type = _report_type_from_name(file_name)
        financial_year = _financial_year_from_name(file_name)

        analyzer_input = _prepare_analyzer_input(pdf_path)
        if analyzer_input is None:
            failed += 1
            logging.error("No extractable text found for %s", pdf_path)
            continue

        opencode_payload = _run_opencode_for_text(
            input_path=analyzer_input["input_path"],
            file_name=file_name,
            report_type=report_type,
            financial_year=financial_year,
        )
        if opencode_payload is None:
            failed += 1
            continue

        normalized = _normalize_parsed_payload(opencode_payload)
        merged_metrics, used_baseline_fallback = _apply_baseline_fallback(normalized["metrics"], stem)
        sanitized_metrics, rejected_metrics, low_quality_text = _sanitize_metrics_with_text(
            merged_metrics,
            analyzer_input["text"],
        )
        final_payload = {
            "symbol": SYMBOL,
            "report_type": report_type,
            "financial_year": financial_year,
            "source_pdf": os.path.relpath(pdf_path, BASE_DIR).replace("\\", "/"),
            "extracted_at": _utc_now_iso(),
            "analyzer": "opencode",
            "analyzer_input": os.path.relpath(analyzer_input["input_path"], BASE_DIR).replace("\\", "/"),
            "analyzer_chars": analyzer_input["char_count"],
            "metrics": sanitized_metrics,
        }
        if used_baseline_fallback:
            final_payload["fallback_source"] = "baseline_parsed"
        if rejected_metrics:
            final_payload["rejected_metrics"] = rejected_metrics
        if low_quality_text:
            final_payload["low_quality_source_text"] = True

        try:
            _write_json_atomic(output_path, final_payload)
            processed += 1
        except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
            failed += 1
            logging.error("Failed to save extracted JSON for %s: %s", pdf_path, str(exc))

    summary = {
        "total_pdfs": len(pdf_files),
        "processed": processed,
        "skipped": skipped,
        "failed": failed,
    }
    _append_summary_record(summary)
    return summary


def main():
    parser = argparse.ArgumentParser(description="Extract report metrics with opencode analyzer.")
    parser.add_argument("--max-files", type=int, default=None, help="Process only the first N PDFs.")
    parser.add_argument("--force", action="store_true", help="Re-extract even when output is up to date.")
    parser.add_argument("--match", type=str, default=None, help="Process only PDFs whose filename contains this text.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    extraction_summary = extract_with_opencode(max_files=args.max_files, force=args.force, match=args.match)

    trusted_summary = None
    try:
        trusted_summary = export_trusted_reports()
    except Exception as exc:
        logging.error("Trusted export failed: %s", str(exc))

    output = {"extraction": extraction_summary, "trusted_export": trusted_summary}
    print(json.dumps(output, ensure_ascii=False))


if __name__ == "__main__":
    main()
