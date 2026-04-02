import datetime
import json
import logging
import os
import re
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

REPORTS_PAGE_URL = "https://www.hayleys.com/annual-reports/"
SYMBOL = "HAYL.N0000"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPORTS_ROOT = os.path.join(BASE_DIR, "data", "raw", "HAYL", "reports")
PDF_DIR = os.path.join(REPORTS_ROOT, "pdfs")
METADATA_DIR = os.path.join(REPORTS_ROOT, "metadata")
TIMEOUT_SECONDS = 20
MAX_RETRIES = 3
CHUNK_SIZE = 1024 * 128
USER_AGENT = "HayleysReportsScraper/1.0 (+https://www.hayleys.com/annual-reports/)"
CATEGORY_NAMES = ("annual_reports", "quarterly_results", "group_company_reports")


def _utc_now_iso():
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def _ensure_output_dirs():
    os.makedirs(PDF_DIR, exist_ok=True)
    os.makedirs(METADATA_DIR, exist_ok=True)


def _request_with_retries(method, url, stream=False, headers=None):
    request_headers = {"User-Agent": USER_AGENT}
    if headers:
        request_headers.update(headers)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.request(
                method=method,
                url=url,
                timeout=TIMEOUT_SECONDS,
                stream=stream,
                allow_redirects=True,
                headers=request_headers,
            )
            response.raise_for_status()
            return response
        except requests.exceptions.Timeout:
            logging.error("Timeout on %s %s (attempt %s/%s)", method, url, attempt, MAX_RETRIES)
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "unknown"
            logging.error("HTTP error on %s %s (attempt %s/%s): %s", method, url, attempt, MAX_RETRIES, status)
        except requests.exceptions.RequestException as exc:
            logging.error("Request error on %s %s (attempt %s/%s): %s", method, url, attempt, MAX_RETRIES, str(exc))
    return None


def fetch_reports_page():
    response = _request_with_retries("GET", REPORTS_PAGE_URL, stream=False)
    if response is None:
        return None
    return response.text


def _content_type_is_pdf(content_type):
    return bool(content_type and "application/pdf" in content_type.lower())


def _url_looks_like_pdf(url):
    path = urlparse(url).path.lower()
    return path.endswith(".pdf")


def _probe_pdf_url(url):
    if _url_looks_like_pdf(url):
        return url

    head_response = _request_with_retries("HEAD", url, stream=False)
    if head_response is not None:
        content_type = head_response.headers.get("content-type", "")
        if _content_type_is_pdf(content_type) or _url_looks_like_pdf(head_response.url):
            return head_response.url

    get_response = _request_with_retries("GET", url, stream=True, headers={"Range": "bytes=0-2048"})
    if get_response is not None:
        try:
            content_type = get_response.headers.get("content-type", "")
            if _content_type_is_pdf(content_type) or _url_looks_like_pdf(get_response.url):
                return get_response.url
        finally:
            get_response.close()

    return None


def _nearest_headings(anchor):
    headings = []
    for tag in anchor.find_all_previous(["h1", "h2", "h3", "h4"]):
        text = " ".join(tag.get_text(" ", strip=True).split())
        if text:
            headings.append(text)
        if len(headings) == 2:
            break
    return headings


def _classify_category(title_text, context_text, url):
    combined = f"{title_text} {context_text} {url}".lower()

    if any(keyword in combined for keyword in ("quarterly", "financial highlights")) or re.search(r"\bq[1-4]\b", combined):
        return "quarterly_results"

    if "annual report" in combined or "view latest annual report" in combined:
        is_hayleys_plc = "hayleys plc" in combined or "hayleys-plc" in combined
        if is_hayleys_plc or "past annual reports" in context_text.lower() or "latest annual report" in combined:
            return "annual_reports"
        return "group_company_reports"

    return None


def _extract_financial_year(text):
    if not text:
        return None

    match = re.search(r"fy\s*([0-9]{2,4})\s*[-/]\s*([0-9]{2,4})", text, flags=re.IGNORECASE)
    if match:
        return f"FY{match.group(1)}-{match.group(2)}"

    match = re.search(r"(?<![0-9])((?:19|20)[0-9]{2})\s*[-/]\s*([0-9]{2})(?![0-9])", text)
    if match:
        return f"FY{match.group(1)}-{match.group(2)}"

    match = re.search(r"(?<![0-9])([0-9]{2})\s*[-/]\s*([0-9]{2})(?![0-9])", text)
    if match:
        return f"FY{match.group(1)}-{match.group(2)}"

    single_year = re.search(r"(?<![0-9])((?:19|20)[0-9]{2})(?![0-9])", text)
    if single_year:
        return f"FY{single_year.group(1)}"

    return None


def _slugify(value):
    cleaned = re.sub(r"[^A-Za-z0-9]+", "_", value.strip().lower())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned or "report"


def normalize_report_name(title, category, financial_year, pdf_url):
    parsed = urlparse(pdf_url)
    source_name = os.path.basename(parsed.path) or "report.pdf"
    stem = os.path.splitext(source_name)[0]
    extension = ".pdf"

    title_text = title or stem
    quarter_match = re.search(r"\b(q[1-4])\b", title_text, flags=re.IGNORECASE) or re.search(
        r"\b(q[1-4])\b", source_name, flags=re.IGNORECASE
    )
    quarter_token = quarter_match.group(1).upper() if quarter_match else None
    fy_token = financial_year if financial_year else None

    if category == "annual_reports":
        if fy_token:
            return f"annual_report_{fy_token}{extension}"
        return f"annual_report_{_slugify(stem)}{extension}"

    if category == "quarterly_results":
        name_parts = ["quarterly"]
        if quarter_token:
            name_parts.append(quarter_token)
        if fy_token:
            name_parts.append(fy_token)
        if len(name_parts) == 1:
            name_parts.append(_slugify(stem))
        return "_".join(name_parts) + extension

    company_name = re.sub(
        r"\b(annual|report|fy|plc|ltd|limited|holdings|results|financial|highlights)\b",
        " ",
        title_text,
        flags=re.IGNORECASE,
    )
    company_name = re.sub(r"(?<![0-9])(?:19|20)[0-9]{2}(?:\s*[-/]\s*[0-9]{2,4})?(?![0-9])", " ", company_name)
    company_name = re.sub(r"\.[Pp][Dd][Ff]\b", " ", company_name)
    company_slug = _slugify(company_name)
    if company_slug == "report":
        company_slug = _slugify(stem)
    if fy_token:
        return f"group_{company_slug}_{fy_token}{extension}"
    return f"group_{company_slug}{extension}"


def parse_report_links(page_html):
    soup = BeautifulSoup(page_html, "html.parser")
    links_by_pdf_url = {}

    for anchor in soup.select("a[href]"):
        href = (anchor.get("href") or "").strip()
        if not href or href.startswith("#"):
            continue

        absolute_url = urljoin(REPORTS_PAGE_URL, href)
        text = " ".join(anchor.stripped_strings).strip()
        headings = _nearest_headings(anchor)
        context = " ".join(headings).strip()
        filename_hint = os.path.splitext(os.path.basename(urlparse(absolute_url).path))[0]
        title = text or (anchor.get("title") or "").strip() or filename_hint

        category = _classify_category(title, context, absolute_url)
        if category not in CATEGORY_NAMES:
            continue

        pdf_url = _probe_pdf_url(absolute_url)
        if not pdf_url:
            continue

        pdf_file_name = os.path.basename(urlparse(pdf_url).path)
        reference_text = " ".join(part for part in (title, context, pdf_file_name) if part)
        financial_year = _extract_financial_year(reference_text)

        entry = {
            "title": title or pdf_file_name,
            "category": category,
            "financial_year": financial_year,
            "source_page": REPORTS_PAGE_URL,
            "pdf_url": pdf_url,
        }

        existing = links_by_pdf_url.get(pdf_url)
        if existing is None:
            links_by_pdf_url[pdf_url] = entry
            continue

        existing_title = existing.get("title", "")
        if existing_title.lower().endswith(".pdf") and text:
            existing["title"] = text
        if not existing.get("financial_year") and financial_year:
            existing["financial_year"] = financial_year

    return list(links_by_pdf_url.values())


def download_pdf(pdf_url, destination_path):
    if os.path.exists(destination_path):
        return "skipped"

    temp_path = destination_path + ".tmp"

    for attempt in range(1, MAX_RETRIES + 1):
        response = None
        try:
            response = requests.get(
                pdf_url,
                stream=True,
                timeout=TIMEOUT_SECONDS,
                allow_redirects=True,
                headers={"User-Agent": USER_AGENT},
            )
            response.raise_for_status()

            content_type = response.headers.get("content-type", "")
            if not (_content_type_is_pdf(content_type) or _url_looks_like_pdf(response.url)):
                logging.error("Skipping non-PDF resource: %s", pdf_url)
                return "invalid"

            os.makedirs(os.path.dirname(destination_path), exist_ok=True)
            with open(temp_path, "wb") as temp_file:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        temp_file.write(chunk)

            if os.path.getsize(temp_path) == 0:
                raise OSError("Downloaded PDF is empty")

            os.replace(temp_path, destination_path)
            return "downloaded"
        except (requests.exceptions.RequestException, OSError) as exc:
            logging.error("Failed to download %s (attempt %s/%s): %s", pdf_url, attempt, MAX_RETRIES, str(exc))
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass
        finally:
            if response is not None:
                response.close()

    return "failed"


def _metadata_file_for(category, downloaded_at):
    date_part = downloaded_at[:10]
    return os.path.join(METADATA_DIR, f"{category}_{date_part}.json")


def _summary_file_for(run_at):
    date_part = run_at[:10]
    return os.path.join(METADATA_DIR, f"scrape_summary_{date_part}.json")


def _append_json_record(file_path, record):
    temp_path = file_path + ".tmp"
    records = []

    if os.path.exists(file_path):
        try:
            with open(file_path, "r", encoding="utf-8") as source_file:
                existing = json.load(source_file)
            if isinstance(existing, list):
                records = existing
        except (json.JSONDecodeError, OSError):
            records = []

    records.append(record)

    payload = json.dumps(records, ensure_ascii=False, indent=2)
    json.loads(payload)

    with open(temp_path, "w", encoding="utf-8") as temp_file:
        temp_file.write(payload)
        temp_file.write("\n")

    with open(temp_path, "r", encoding="utf-8") as verify_file:
        json.load(verify_file)

    os.replace(temp_path, file_path)


def _metadata_record_exists(category, pdf_url, local_path):
    if not os.path.isdir(METADATA_DIR):
        return False

    prefix = f"{category}_"
    for file_name in os.listdir(METADATA_DIR):
        if not (file_name.startswith(prefix) and file_name.endswith(".json")):
            continue

        file_path = os.path.join(METADATA_DIR, file_name)
        try:
            with open(file_path, "r", encoding="utf-8") as source_file:
                existing = json.load(source_file)
        except (json.JSONDecodeError, OSError):
            continue

        if not isinstance(existing, list):
            continue

        for entry in existing:
            if not isinstance(entry, dict):
                continue
            if entry.get("pdf_url") == pdf_url or entry.get("local_path") == local_path:
                return True

    return False


def save_report_metadata(metadata_record):
    if _metadata_record_exists(
        category=metadata_record["category"],
        pdf_url=metadata_record["pdf_url"],
        local_path=metadata_record["local_path"],
    ):
        return False

    file_path = _metadata_file_for(metadata_record["category"], metadata_record["downloaded_at"])
    _append_json_record(file_path, metadata_record)
    return True


def save_scrape_summary(summary):
    run_record = {
        "run_at": _utc_now_iso(),
        "symbol": SYMBOL,
        "source_page": REPORTS_PAGE_URL,
        "total_links_found": summary.get("total_links_found", 0),
        "pdfs_downloaded": summary.get("pdfs_downloaded", 0),
        "pdfs_skipped": summary.get("pdfs_skipped", 0),
        "metadata_records_written": summary.get("metadata_records_written", 0),
    }
    file_path = _summary_file_for(run_record["run_at"])
    _append_json_record(file_path, run_record)


def scrape_and_store_reports():
    _ensure_output_dirs()
    page_html = fetch_reports_page()
    if not page_html:
        return {
            "total_links_found": 0,
            "pdfs_downloaded": 0,
            "pdfs_skipped": 0,
            "metadata_records_written": 0,
        }

    report_links = parse_report_links(page_html)
    downloaded_count = 0
    skipped_count = 0
    metadata_written = 0

    for report in report_links:
        file_name = normalize_report_name(
            title=report["title"],
            category=report["category"],
            financial_year=report["financial_year"],
            pdf_url=report["pdf_url"],
        )
        destination_path = os.path.join(PDF_DIR, file_name)
        result = download_pdf(report["pdf_url"], destination_path)

        if result == "downloaded":
            downloaded_count += 1
        elif result == "skipped":
            skipped_count += 1
        else:
            continue

        metadata_record = {
            "title": report["title"],
            "category": report["category"],
            "financial_year": report["financial_year"],
            "source_page": report["source_page"],
            "pdf_url": report["pdf_url"],
            "local_path": os.path.relpath(destination_path, BASE_DIR).replace("\\", "/"),
            "downloaded_at": _utc_now_iso(),
        }

        try:
            wrote = save_report_metadata(metadata_record)
            if wrote:
                metadata_written += 1
        except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
            logging.error("Failed to save metadata for %s: %s", report["pdf_url"], str(exc))

    return {
        "total_links_found": len(report_links),
        "pdfs_downloaded": downloaded_count,
        "pdfs_skipped": skipped_count,
        "metadata_records_written": metadata_written,
    }


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    summary = scrape_and_store_reports()
    save_scrape_summary(summary)
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
