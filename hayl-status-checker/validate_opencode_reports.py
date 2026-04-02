import datetime
import json
import os
import re
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPORTS_DIR = os.path.join(BASE_DIR, "data", "raw", "HAYL", "reports")
PARSED_DIR = os.path.join(REPORTS_DIR, "parsed_opencode")
METADATA_DIR = os.path.join(REPORTS_DIR, "metadata")

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

METRIC_HINTS = {
    "revenue": ("revenue", "turnover", "sales"),
    "net_income": ("net income", "profit after tax", "pat", "profit for the year"),
    "eps": ("eps", "earnings per share"),
    "operating_profit": ("operating profit", "operating income", "results from operating"),
    "ebitda": ("ebitda",),
    "issued_shares": ("issued shares", "no. of shares", "shares"),
    "par_value": ("par value",),
    "market_cap": ("market capitalisation", "market capitalization", "market cap"),
    "foreign_percentage": ("foreign", "foreign percentage"),
    "pe_ratio": ("p/e", "price earnings", "pe ratio"),
    "pb_ratio": ("p/b", "price book", "pb ratio"),
    "dividend_yield": ("dividend yield",),
}

NUMBER_PATTERN = re.compile(r"\(?-?\d[\d,]*(?:\.\d+)?\)?")


def _utc_now_iso():
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_number(value):
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if not isinstance(value, str):
        return None
    cleaned = value.strip().replace(",", "")
    if not cleaned:
        return None
    negative = cleaned.startswith("(") and cleaned.endswith(")")
    cleaned = cleaned.replace("(", "").replace(")", "")
    try:
        parsed = float(cleaned)
        return -parsed if negative else parsed
    except ValueError:
        return None


def _normalize_unit(metric, unit):
    if unit is None:
        return "none"
    text = str(unit).strip().lower()
    compact = re.sub(r"[\s\.\-_/]", "", text)

    if any(token in text for token in ("%", "percent")):
        return "percent"
    if any(token in text for token in ("times", "ratio")):
        return "ratio"
    if "share" in text and metric == "eps":
        return "lkr_per_share"

    if metric == "issued_shares":
        if "million" in text or "mn" in text:
            return "shares_mn"
        if "share" in text or "no" in text:
            return "shares"
        return "unknown"

    if metric in ("pe_ratio", "pb_ratio", "dividend_yield"):
        if metric == "dividend_yield":
            return "percent" if "percent" in text or "%" in text else "unknown"
        return "ratio" if any(token in text for token in ("times", "ratio")) else "unknown"

    if "bn" in text:
        return "lkr_bn"
    if "mn" in text or "million" in text:
        return "lkr_mn"
    if "000" in compact:
        return "lkr_thousand"
    if "lkr" in text or "rs" in text:
        return "lkr"

    return "unknown"


def _value_candidates(value):
    if value is None:
        return set()

    numeric = float(value)
    rounded_int = int(round(numeric))
    candidates = {
        str(numeric),
        format(numeric, ".2f").rstrip("0").rstrip("."),
        format(rounded_int, "d"),
        format(rounded_int, ",d"),
    }

    if abs(numeric) >= 1000:
        candidates.add(format(numeric, ",.2f").rstrip("0").rstrip("."))
    return {candidate for candidate in candidates if candidate}


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


def _has_close_number(line, expected_value):
    expected = float(expected_value)
    tolerance = max(0.01, abs(expected) * 0.005)
    for token in NUMBER_PATTERN.findall(line):
        parsed = _parse_number(token)
        if parsed is None:
            continue
        if abs(parsed - expected) <= tolerance:
            return True
    return False


def _load_json(path):
    with open(path, "r", encoding="utf-8") as source:
        return json.load(source)


def _write_json_atomic(path, payload):
    temp_path = path + ".tmp"
    serialized = json.dumps(payload, ensure_ascii=False, indent=2)
    json.loads(serialized)
    with open(temp_path, "w", encoding="utf-8") as target:
        target.write(serialized)
        target.write("\n")
    with open(temp_path, "r", encoding="utf-8") as verify:
        json.load(verify)
    os.replace(temp_path, path)


def _validation_path(run_at):
    return os.path.join(METADATA_DIR, f"opencode_validation_{run_at[:10]}.json")


def validate_reports():
    os.makedirs(METADATA_DIR, exist_ok=True)
    files = sorted(
        os.path.join(PARSED_DIR, name)
        for name in os.listdir(PARSED_DIR)
        if name.lower().endswith(".json")
    ) if os.path.isdir(PARSED_DIR) else []

    all_null_reports = []
    unit_issues = []
    suspicious_values = []
    unit_sets = defaultdict(set)

    for path in files:
        report_name = os.path.basename(path)
        parsed = _load_json(path)
        metrics = parsed.get("metrics", {})
        non_null_count = 0

        analyzer_input_rel = parsed.get("analyzer_input")
        analyzer_text = ""
        if isinstance(analyzer_input_rel, str):
            analyzer_path = os.path.join(BASE_DIR, analyzer_input_rel.replace("/", os.sep))
            if os.path.exists(analyzer_path):
                try:
                    with open(analyzer_path, "r", encoding="utf-8") as source:
                        analyzer_text = source.read()
                except OSError:
                    analyzer_text = ""

        for metric in METRIC_KEYS:
            entry = metrics.get(metric) if isinstance(metrics.get(metric), dict) else {}
            value = entry.get("value")
            unit = entry.get("unit")

            if value is not None:
                non_null_count += 1
                normalized_unit = _normalize_unit(metric, unit)
                unit_sets[metric].add(normalized_unit)

                if normalized_unit in ("unknown", "none"):
                    unit_issues.append(
                        {
                            "report": report_name,
                            "metric": metric,
                            "unit": unit,
                            "normalized_unit": normalized_unit,
                            "reason": "unit_missing_or_unrecognized_for_non_null_value",
                        }
                    )

                if analyzer_text:
                    candidates = _value_candidates(value)
                    global_match = any(candidate in analyzer_text for candidate in candidates)
                    line_matches = _extract_line_numbers(analyzer_text, metric)
                    keyword_line_match = any(_has_close_number(line, value) for _, line in line_matches)

                    if not (keyword_line_match or global_match):
                        suspicious_values.append(
                            {
                                "report": report_name,
                                "metric": metric,
                                "value": value,
                                "unit": unit,
                                "reason": "value_not_found_in_analyzer_input_text",
                            }
                        )

        if non_null_count == 0:
            all_null_reports.append(report_name)

    cross_report_unit_inconsistencies = []
    for metric, units in sorted(unit_sets.items()):
        clean_units = sorted(unit for unit in units if unit not in ("none", "unknown"))
        if len(clean_units) > 1:
            cross_report_unit_inconsistencies.append(
                {
                    "metric": metric,
                    "normalized_units": clean_units,
                }
            )

    return {
        "generated_at": _utc_now_iso(),
        "total_reports": len(files),
        "summary": {
            "all_null_report_count": len(all_null_reports),
            "unit_issue_count": len(unit_issues),
            "cross_report_unit_inconsistency_count": len(cross_report_unit_inconsistencies),
            "suspicious_value_count": len(suspicious_values),
        },
        "all_null_reports": all_null_reports,
        "unit_issues": unit_issues,
        "cross_report_unit_inconsistencies": cross_report_unit_inconsistencies,
        "suspicious_values": suspicious_values,
    }


def main():
    result = validate_reports()
    output_path = _validation_path(result["generated_at"])
    _write_json_atomic(output_path, result)
    print(json.dumps({"validation_file": os.path.relpath(output_path, BASE_DIR).replace("\\", "/"), "summary": result["summary"]}))


if __name__ == "__main__":
    main()
