import argparse
import datetime
import json
import logging
import os
import re
import shutil
import subprocess

import pdfplumber

SYMBOL = "HAYL.N0000"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPORTS_DIR = os.path.join(BASE_DIR, "data", "raw", "HAYL", "reports")
PDF_DIR = os.path.join(REPORTS_DIR, "pdfs")
PARSED_DIR = os.path.join(REPORTS_DIR, "parsed_opencode")
METADATA_DIR = os.path.join(REPORTS_DIR, "metadata")
ANALYZER_INPUT_DIR = os.path.join(REPORTS_DIR, "analyzer_input")
OPENCODE_TIMEOUT_SECONDS = 420
MAX_OPENCODE_RETRIES = 2
ANSI_PATTERN = re.compile(r"\x1b\[[0-9;]*m")
MAX_ANALYZER_PAGES = 40
MAX_ANALYZER_CHARS = 70000

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


def _normalize_metric(metric):
    if not isinstance(metric, dict):
        metric = {}
    return {
        "value": _to_number(metric.get("value")),
        "unit": metric.get("unit") if isinstance(metric.get("unit"), str) else None,
        "page": _to_int(metric.get("page")),
        "confidence": _to_number(metric.get("confidence")),
    }


def _normalize_parsed_payload(payload):
    metrics = payload.get("metrics", {}) if isinstance(payload, dict) else {}
    normalized_metrics = {}
    for key in METRIC_KEYS:
        normalized_metrics[key] = _normalize_metric(metrics.get(key))
    return {"metrics": normalized_metrics}


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
    chunks = []
    current_chars = 0
    page_count = 0

    with pdfplumber.open(pdf_path) as pdf:
        page_count = len(pdf.pages)
        for page_index, page in enumerate(pdf.pages, start=1):
            if page_index > MAX_ANALYZER_PAGES or current_chars >= MAX_ANALYZER_CHARS:
                break

            page_lines = [f"[Page {page_index}]"]
            text = page.extract_text() or ""
            if text.strip():
                page_lines.append(text.strip())

            try:
                tables = page.extract_tables() or []
            except Exception:
                tables = []

            for table in tables[:4]:
                page_lines.append("[Table]")
                for row in table[:20]:
                    if not isinstance(row, list):
                        continue
                    row_values = [str(cell).strip() for cell in row if cell not in (None, "")]
                    if row_values:
                        page_lines.append(" | ".join(row_values))

            page_block = "\n".join(page_lines).strip()
            if not page_block:
                continue

            remaining = MAX_ANALYZER_CHARS - current_chars
            if len(page_block) > remaining:
                page_block = page_block[:remaining]
            chunks.append(page_block)
            current_chars += len(page_block)

            if current_chars >= MAX_ANALYZER_CHARS:
                break

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
            return {"input_path": input_path, "page_count": None, "char_count": len(text)}
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


def extract_with_opencode(max_files=None, force=False):
    _ensure_output_dirs()
    if not os.path.isdir(PDF_DIR):
        return {"total_pdfs": 0, "processed": 0, "skipped": 0, "failed": 0}

    pdf_files = sorted(
        os.path.join(PDF_DIR, name)
        for name in os.listdir(PDF_DIR)
        if name.lower().endswith(".pdf")
    )
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
        final_payload = {
            "symbol": SYMBOL,
            "report_type": report_type,
            "financial_year": financial_year,
            "source_pdf": os.path.relpath(pdf_path, BASE_DIR).replace("\\", "/"),
            "extracted_at": _utc_now_iso(),
            "analyzer": "opencode",
            "analyzer_input": os.path.relpath(analyzer_input["input_path"], BASE_DIR).replace("\\", "/"),
            "analyzer_chars": analyzer_input["char_count"],
            "metrics": normalized["metrics"],
        }

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
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    summary = extract_with_opencode(max_files=args.max_files, force=args.force)
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
