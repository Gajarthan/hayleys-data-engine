import requests
import json
import datetime
import os
import logging

SYMBOL = "HAYL.N0000"
API_URL = "https://www.cse.lk/api/companyInfoSummery"
REQUEST_TIMEOUT_SECONDS = 10
MAX_RETRIES = 3
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
SECTION_NAMES = ("price", "financials", "balance", "valuation", "reports")


def _symbol_root(symbol):
    return str(symbol).split(".")[0]


def create_data_folder(symbol=SYMBOL):
    symbol_dir = os.path.join(RAW_DIR, _symbol_root(symbol))
    os.makedirs(symbol_dir, exist_ok=True)
    for section in SECTION_NAMES:
        os.makedirs(os.path.join(symbol_dir, section), exist_ok=True)


def fetch_stock_data(symbol=SYMBOL):
    payload = {"symbol": symbol}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(API_URL, data=payload, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            try:
                return response.json()
            except ValueError:
                logging.error("Malformed JSON response on attempt %s/%s", attempt, MAX_RETRIES)
        except requests.exceptions.Timeout:
            logging.error("Timeout on attempt %s/%s", attempt, MAX_RETRIES)
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "unknown"
            logging.error("HTTP error on attempt %s/%s: %s", attempt, MAX_RETRIES, status)
        except requests.exceptions.RequestException as exc:
            logging.error("Request error on attempt %s/%s: %s", attempt, MAX_RETRIES, str(exc))

    return None


def _safe_number(value):
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().replace(",", "").replace("%", "")
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _safe_int(value):
    numeric = _safe_number(value)
    if numeric is None:
        return None
    return int(numeric)


def _find_value(raw_response, keys):
    if not isinstance(keys, (list, tuple)):
        return None

    normalized = {key.lower() for key in keys if isinstance(key, str)}
    stack = [raw_response]

    while stack:
        node = stack.pop()

        if isinstance(node, dict):
            for key, value in node.items():
                if isinstance(key, str) and key.lower() in normalized and value not in (None, ""):
                    return value
            for value in node.values():
                if isinstance(value, (dict, list)):
                    stack.append(value)
        elif isinstance(node, list):
            for item in node:
                if isinstance(item, (dict, list)):
                    stack.append(item)

    return None


def _extract_section(raw_response, key):
    if not isinstance(raw_response, dict):
        return {}

    section = raw_response.get(key)
    if isinstance(section, dict):
        return section
    if isinstance(section, list):
        for item in section:
            if isinstance(item, dict):
                return item
    return {}


def _to_iso_date(value):
    if value in (None, ""):
        return None

    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        return value.isoformat()

    if isinstance(value, datetime.datetime):
        return value.date().isoformat()

    if not isinstance(value, str):
        return None

    raw = value.strip()
    if not raw:
        return None

    try:
        normalized = raw.replace("Z", "+00:00")
        return datetime.datetime.fromisoformat(normalized).date().isoformat()
    except ValueError:
        pass

    date_formats = (
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%d/%b/%Y",
        "%Y/%m/%d",
        "%d.%m.%Y",
        "%b %d, %Y",
        "%d %b %Y",
        "%Y-%m-%d %H:%M:%S",
    )

    for date_format in date_formats:
        try:
            return datetime.datetime.strptime(raw, date_format).date().isoformat()
        except ValueError:
            continue

    return None


def _float_from(source, keys):
    return _safe_number(_find_value(source, keys))


def _int_from(source, keys):
    return _safe_int(_find_value(source, keys))


def transform_price_data(symbol, company, symbol_info, fetched_at):
    return {
        "symbol": symbol,
        "company": company,
        "last_price": _float_from(symbol_info, ["lastPrice", "last_price", "lastTradedPrice", "ltp", "price", "last"]),
        "previous_close": _float_from(
            symbol_info, ["previousClose", "previous_close", "previousClosingPrice", "prevClose", "close"]
        ),
        "change_value": _float_from(symbol_info, ["change", "changeValue", "change_value", "priceChange"]),
        "change_percent": _float_from(
            symbol_info, ["changePercent", "change_percent", "changePercentage", "changePct"]
        ),
        "high": {
            "today": _float_from(symbol_info, ["todayHigh", "highToday", "dayHigh", "high", "hiTrade"]),
            "week": _float_from(symbol_info, ["weekHigh", "highWeek", "wtdHiPrice"]),
            "month": _float_from(symbol_info, ["monthHigh", "highMonth", "mtdHiPrice"]),
            "year": _float_from(symbol_info, ["yearHigh", "highYear", "ytdHigh", "ytdHiPrice"]),
            "52_week": _float_from(symbol_info, ["week52High", "high52Week", "fiftyTwoWeekHigh", "p12HiPrice"]),
            "all_time": _float_from(symbol_info, ["allTimeHigh"]),
        },
        "low": {
            "today": _float_from(symbol_info, ["todayLow", "lowToday", "dayLow", "low", "lowTrade"]),
            "week": _float_from(symbol_info, ["weekLow", "lowWeek", "wtdLowPrice"]),
            "month": _float_from(symbol_info, ["monthLow", "lowMonth", "mtdLowPrice"]),
            "year": _float_from(symbol_info, ["yearLow", "lowYear", "ytdLow", "ytdLowPrice"]),
            "52_week": _float_from(symbol_info, ["week52Low", "low52Week", "fiftyTwoWeekLow", "p12LowPrice"]),
            "all_time": _float_from(symbol_info, ["allTimeLow"]),
        },
        "volume": {
            "today": _int_from(symbol_info, ["todayVolume", "volumeToday", "volume", "tradeVolume", "tdyShareVolume"]),
            "week": _int_from(symbol_info, ["weekVolume", "volumeWeek", "wtdShareVolume", "wdyShareVolume"]),
            "month": _int_from(symbol_info, ["monthVolume", "volumeMonth", "mtdShareVolume"]),
            "year": _int_from(symbol_info, ["yearVolume", "volumeYear", "ytdShareVolume"]),
            "52_week": _int_from(symbol_info, ["week52Volume", "volume52Week", "fiftyTwoWeekVolume", "p12ShareVolume"]),
        },
        "trades": {
            "today": _int_from(symbol_info, ["todayTrades", "tradesToday", "numberOfTrades", "trades", "tdyTradeVolume"]),
        },
        "turnover": {
            "today": _float_from(symbol_info, ["todayTurnover", "turnoverToday", "turnover", "tdyTurnover"]),
            "week": _float_from(symbol_info, ["weekTurnover", "turnoverWeek", "wtdTurnover"]),
            "month": _float_from(symbol_info, ["monthTurnover", "turnoverMonth", "mtdTurnover"]),
            "year": _float_from(symbol_info, ["yearTurnover", "turnoverYear", "ytdTurnover"]),
        },
        "fetched_at": fetched_at,
    }


def transform_financials_data(symbol, company, symbol_info, fetched_at):
    return {
        "symbol": symbol,
        "company": company,
        "revenue": _float_from(symbol_info, ["revenue", "totalRevenue", "sales"]),
        "net_income": _float_from(symbol_info, ["netIncome", "net_income", "profitAfterTax"]),
        "eps": _float_from(symbol_info, ["eps", "earningsPerShare"]),
        "operating_profit": _float_from(symbol_info, ["operatingProfit", "operating_profit"]),
        "ebitda": _float_from(symbol_info, ["ebitda"]),
        "fetched_at": fetched_at,
    }


def transform_balance_data(symbol, company, symbol_info, fetched_at):
    return {
        "symbol": symbol,
        "company": company,
        "issued_shares": _int_from(symbol_info, ["issuedShares", "issued_shares", "numberOfShares", "quantityIssued"]),
        "par_value": _float_from(symbol_info, ["parValue", "par_value"]),
        "market_cap": _float_from(symbol_info, ["marketCap", "market_cap", "marketCapitalization"]),
        "market_cap_percent": _float_from(
            symbol_info, ["marketCapPercent", "market_cap_percent", "marketCapPercentage"]
        ),
        "foreign_holdings": _float_from(symbol_info, ["foreignHoldings", "foreign_holdings"]),
        "foreign_percentage": _float_from(symbol_info, ["foreignPercentage", "foreign_percentage"]),
        "fetched_at": fetched_at,
    }


def transform_valuation_data(symbol, company, symbol_info, beta_info, fetched_at):
    return {
        "symbol": symbol,
        "company": company,
        "beta": {
            "triASI": _float_from(beta_info, ["triASI", "betaTriASI", "betaTriAsi", "triASIBetaValue"]),
            "SPSL": _float_from(beta_info, ["SPSL", "betaSPSL", "betaSpsl", "betaValueSPSL"]),
            "period": _find_value(beta_info, ["period", "betaPeriod", "triASIBetaPeriod"]),
            "quarter": _int_from(beta_info, ["quarter", "betaQuarter"]),
        },
        "pe_ratio": _float_from(symbol_info, ["peRatio", "pe_ratio", "priceEarningsRatio"]),
        "pb_ratio": _float_from(symbol_info, ["pbRatio", "pb_ratio", "priceBookRatio"]),
        "dividend_yield": _float_from(symbol_info, ["dividendYield", "dividend_yield"]),
        "fetched_at": fetched_at,
    }


def transform_reports_data(symbol, company, symbol_info, logo_info, tag_logo_info, fetched_at):
    company_logo = _find_value(logo_info, ["company_logo", "companyLogo", "logo", "url", "path"])
    tag_logo = _find_value(tag_logo_info, ["tag_logo", "tagLogo", "logo", "url", "path"])

    return {
        "symbol": symbol,
        "company": company,
        "isin": _find_value(symbol_info, ["isin", "isinCode"]),
        "issue_date": _to_iso_date(_find_value(symbol_info, ["issueDate", "issue_date", "listedDate"])),
        "logos": {
            "company_logo": company_logo,
            "tag_logo": tag_logo,
        },
        "security_id": _int_from(
            {"symbol_info": symbol_info, "logo_info": logo_info},
            ["securityID", "securityId", "security_id", "symbolID", "secId"],
        ),
        "fetched_at": fetched_at,
    }


def transform_all_data(raw_response):
    symbol_info = _extract_section(raw_response, "reqSymbolInfo")
    beta_info = _extract_section(raw_response, "reqSymbolBetaInfo")
    logo_info = _extract_section(raw_response, "reqLogo")
    tag_logo_info = _extract_section(raw_response, "reqTagsLogo")

    symbol = _find_value(symbol_info, ["symbol", "symbolCode", "ticker", "securitySymbol"]) or SYMBOL
    company = _find_value(symbol_info, ["name", "companyName", "securityName", "symbolName", "shortName"])
    fetched_at = datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")

    return {
        "price": transform_price_data(symbol, company, symbol_info, fetched_at),
        "financials": transform_financials_data(symbol, company, symbol_info, fetched_at),
        "balance": transform_balance_data(symbol, company, symbol_info, fetched_at),
        "valuation": transform_valuation_data(symbol, company, symbol_info, beta_info, fetched_at),
        "reports": transform_reports_data(symbol, company, symbol_info, logo_info, tag_logo_info, fetched_at),
    }


def parse_stock_data(raw_response):
    return transform_all_data(raw_response)


def append_to_json_file(record, category="price", symbol=SYMBOL):
    date_part = record["fetched_at"][:10]
    file_name = f"{symbol}_{date_part}.json"
    category_dir = os.path.join(RAW_DIR, _symbol_root(symbol), category)
    os.makedirs(category_dir, exist_ok=True)
    file_path = os.path.join(category_dir, file_name)
    temp_path = file_path + ".tmp"

    entries = []

    if os.path.exists(file_path):
        try:
            with open(file_path, "r", encoding="utf-8") as source_file:
                existing_data = json.load(source_file)
            if isinstance(existing_data, list):
                entries = existing_data
            else:
                logging.error("Existing data in %s is not a JSON array; starting new array", file_path)
        except (json.JSONDecodeError, OSError) as exc:
            logging.error("Could not read existing file %s (%s); starting new array", file_path, str(exc))

    entries.append(record)

    try:
        payload = json.dumps(entries, indent=2, ensure_ascii=False)
        json.loads(payload)

        with open(temp_path, "w", encoding="utf-8") as target_file:
            target_file.write(payload)
            target_file.write("\n")

        with open(temp_path, "r", encoding="utf-8") as verify_file:
            json.load(verify_file)

        os.replace(temp_path, file_path)
        return True
    except (TypeError, ValueError, OSError) as exc:
        logging.error("Failed to write JSON file %s (%s)", file_path, str(exc))
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass
        return False


def save_category_data(all_data):
    saved_all = True
    for category in SECTION_NAMES:
        record = all_data.get(category)
        if not isinstance(record, dict):
            logging.error("Missing or invalid transformed object for category: %s", category)
            saved_all = False
            continue
        if not append_to_json_file(record, category=category, symbol=SYMBOL):
            saved_all = False
    return saved_all


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    create_data_folder(SYMBOL)
    raw_response = fetch_stock_data(SYMBOL)
    if raw_response is None:
        logging.error("No data fetched for %s", SYMBOL)
        return

    transformed = parse_stock_data(raw_response)
    timestamp = transformed.get("price", {}).get("fetched_at") or datetime.datetime.now(datetime.timezone.utc).isoformat()

    if save_category_data(transformed):
        logging.info("Stored %s at %s", SYMBOL, timestamp)
        print(f"Stored {SYMBOL} at {timestamp}")
    else:
        logging.error("Failed to fully store data for %s", SYMBOL)


if __name__ == "__main__":
    main()
