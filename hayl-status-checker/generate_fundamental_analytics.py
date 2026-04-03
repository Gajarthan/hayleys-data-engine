import datetime
import json
import logging
import os
import re

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPORTS_DIR = os.path.join(BASE_DIR, "data", "raw", "HAYL", "reports")
TRUSTED_DIR = os.path.join(REPORTS_DIR, "parsed_trusted")
ANALYTICS_DIR = os.path.join(REPORTS_DIR, "analytics")
DATASET_PATH = os.path.join(TRUSTED_DIR, "trusted_reports_dataset.json")

CORE_METRIC_KEYS = {
    "revenue": "revenue",
    "net_profit": "net_income",
    "operating_margin": "operating_margin",
    "free_cash_flow": "free_cash_flow",
    "eps": "eps",
    "roe": "roe",
    "debt_to_equity": "debt_to_equity",
}


def _utc_now_iso():
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


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


def _append_json_array(file_path, record):
    existing = []
    if os.path.exists(file_path):
        try:
            with open(file_path, "r", encoding="utf-8") as source_file:
                loaded = json.load(source_file)
            if isinstance(loaded, list):
                existing = loaded
        except (json.JSONDecodeError, OSError):
            existing = []

    existing.append(record)
    _write_json_atomic(file_path, existing)


def _safe_number(value):
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().replace(",", "")
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _metric_value(record, metric_key):
    metrics = record.get("metrics", {})
    metric = metrics.get(metric_key)
    if not isinstance(metric, dict):
        return None
    return _safe_number(metric.get("value"))


def _financial_year_sort_key(financial_year):
    if not isinstance(financial_year, str):
        return 0
    match = re.search(r"(19|20)\d{2}", financial_year)
    if not match:
        return 0
    return int(match.group(0))


def _coverage_report(rows):
    total = len(rows)
    result = {}
    for label, metric_key in CORE_METRIC_KEYS.items():
        non_null = sum(1 for row in rows if row.get(metric_key) is not None)
        coverage = (non_null / total * 100.0) if total > 0 else 0.0
        result[label] = {
            "metric_key": metric_key,
            "total_records": total,
            "non_null_records": non_null,
            "coverage_pct": round(coverage, 2),
        }
    return result


def _yoy(curr, prev):
    if curr is None or prev is None or prev == 0:
        return None
    return round(((curr - prev) / abs(prev)) * 100.0, 2)


def _build_rows(dataset):
    annual = [item for item in dataset if item.get("report_type") == "annual_reports"]
    annual.sort(key=lambda item: _financial_year_sort_key(item.get("financial_year")))

    rows = []
    for item in annual:
        revenue = _metric_value(item, "revenue")
        operating_profit = _metric_value(item, "operating_profit")
        operating_margin = None
        if revenue not in (None, 0) and operating_profit is not None:
            operating_margin = round((operating_profit / revenue) * 100.0, 2)

        rows.append(
            {
                "financial_year": item.get("financial_year"),
                "report_type": item.get("report_type"),
                "revenue": revenue,
                "net_income": _metric_value(item, "net_income"),
                "eps": _metric_value(item, "eps"),
                "operating_profit": operating_profit,
                "operating_margin": operating_margin,
                "free_cash_flow": _metric_value(item, "free_cash_flow"),
                "roe": _metric_value(item, "roe"),
                "debt_to_equity": _metric_value(item, "debt_to_equity"),
                "source_pdf": item.get("source_pdf"),
            }
        )

    return rows


def _skeptical_checks(rows):
    revenue_margin_flags = []
    profit_fcf_flags = []

    for prev, curr in zip(rows, rows[1:]):
        fy = curr.get("financial_year")
        prev_rev = prev.get("revenue")
        curr_rev = curr.get("revenue")
        prev_margin = prev.get("operating_margin")
        curr_margin = curr.get("operating_margin")

        if (
            prev_rev is not None
            and curr_rev is not None
            and prev_margin is not None
            and curr_margin is not None
            and curr_rev > prev_rev
            and curr_margin < prev_margin
        ):
            revenue_margin_flags.append(
                {
                    "financial_year": fy,
                    "previous_revenue": prev_rev,
                    "current_revenue": curr_rev,
                    "previous_operating_margin_pct": prev_margin,
                    "current_operating_margin_pct": curr_margin,
                }
            )

        prev_profit = prev.get("net_income")
        curr_profit = curr.get("net_income")
        prev_fcf = prev.get("free_cash_flow")
        curr_fcf = curr.get("free_cash_flow")
        if (
            prev_profit is not None
            and curr_profit is not None
            and prev_fcf is not None
            and curr_fcf is not None
            and curr_profit > prev_profit
            and curr_fcf < prev_fcf
        ):
            profit_fcf_flags.append(
                {
                    "financial_year": fy,
                    "previous_profit": prev_profit,
                    "current_profit": curr_profit,
                    "previous_free_cash_flow": prev_fcf,
                    "current_free_cash_flow": curr_fcf,
                }
            )

    return {
        "profit_up_cashflow_down": {
            "available": any(row.get("free_cash_flow") is not None for row in rows),
            "flagged_count": len(profit_fcf_flags),
            "flags": profit_fcf_flags,
        },
        "revenue_up_margin_down": {
            "available": any(row.get("operating_margin") is not None for row in rows),
            "flagged_count": len(revenue_margin_flags),
            "flags": revenue_margin_flags,
        },
    }


def _quality_score(coverage, checks, latest_row):
    score = 100

    critical_keys = ("revenue", "net_profit", "operating_margin", "free_cash_flow")
    for key in critical_keys:
        if coverage[key]["coverage_pct"] < 40:
            score -= 12
        elif coverage[key]["coverage_pct"] < 70:
            score -= 6

    score -= checks["revenue_up_margin_down"]["flagged_count"] * 5
    score -= checks["profit_up_cashflow_down"]["flagged_count"] * 8

    if latest_row:
        if latest_row.get("net_income") is not None and latest_row["net_income"] <= 0:
            score -= 10
        if latest_row.get("operating_margin") is not None and latest_row["operating_margin"] < 5:
            score -= 8
        if latest_row.get("debt_to_equity") is not None and latest_row["debt_to_equity"] > 2.5:
            score -= 5

    if score < 0:
        score = 0
    return int(round(score))


def generate_fundamental_analytics():
    os.makedirs(ANALYTICS_DIR, exist_ok=True)
    run_at = _utc_now_iso()

    if not os.path.exists(DATASET_PATH):
        summary = {
            "run_at": run_at,
            "symbol": "HAYL.N0000",
            "source_dataset_exists": False,
            "records_analyzed": 0,
            "annual_records_analyzed": 0,
            "message": "trusted_reports_dataset.json not found",
        }
        _write_json_atomic(os.path.join(ANALYTICS_DIR, "fundamental_analytics_latest.json"), summary)
        _append_json_array(os.path.join(ANALYTICS_DIR, f"fundamental_analytics_{run_at[:10]}.json"), summary)
        return summary

    with open(DATASET_PATH, "r", encoding="utf-8") as source_file:
        dataset = json.load(source_file)

    if not isinstance(dataset, list):
        raise ValueError("trusted_reports_dataset.json must be a JSON array")

    rows = _build_rows(dataset)
    coverage = _coverage_report(rows)
    checks = _skeptical_checks(rows)
    latest_row = rows[-1] if rows else None
    previous_row = rows[-2] if len(rows) >= 2 else None

    latest = None
    yoy = None
    if latest_row:
        latest = {
            "financial_year": latest_row.get("financial_year"),
            "revenue": latest_row.get("revenue"),
            "net_profit": latest_row.get("net_income"),
            "operating_margin_pct": latest_row.get("operating_margin"),
            "free_cash_flow": latest_row.get("free_cash_flow"),
            "eps": latest_row.get("eps"),
            "roe": latest_row.get("roe"),
            "debt_to_equity": latest_row.get("debt_to_equity"),
        }
    if latest_row and previous_row:
        yoy = {
            "financial_year": latest_row.get("financial_year"),
            "revenue_growth_pct": _yoy(latest_row.get("revenue"), previous_row.get("revenue")),
            "net_profit_growth_pct": _yoy(latest_row.get("net_income"), previous_row.get("net_income")),
            "eps_growth_pct": _yoy(latest_row.get("eps"), previous_row.get("eps")),
            "operating_margin_change_pct_point": (
                round(latest_row["operating_margin"] - previous_row["operating_margin"], 2)
                if latest_row.get("operating_margin") is not None and previous_row.get("operating_margin") is not None
                else None
            ),
        }

    analytics = {
        "run_at": run_at,
        "symbol": "HAYL.N0000",
        "source_dataset_exists": True,
        "source_dataset_path": os.path.relpath(DATASET_PATH, BASE_DIR).replace("\\", "/"),
        "records_analyzed": len(dataset),
        "annual_records_analyzed": len(rows),
        "coverage": coverage,
        "latest": latest,
        "year_over_year": yoy,
        "skeptical_checks": checks,
        "quality_score": _quality_score(coverage, checks, latest_row),
    }

    latest_path = os.path.join(ANALYTICS_DIR, "fundamental_analytics_latest.json")
    daily_path = os.path.join(ANALYTICS_DIR, f"fundamental_analytics_{run_at[:10]}.json")
    _write_json_atomic(latest_path, analytics)
    _append_json_array(daily_path, analytics)

    return analytics


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    analytics = generate_fundamental_analytics()
    print(
        json.dumps(
            {
                "symbol": analytics.get("symbol"),
                "run_at": analytics.get("run_at"),
                "records_analyzed": analytics.get("records_analyzed"),
                "annual_records_analyzed": analytics.get("annual_records_analyzed"),
                "quality_score": analytics.get("quality_score"),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
