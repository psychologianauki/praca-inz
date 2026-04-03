"""
Oil prices data controller - scraping via EIA HTML table (no file download required)
"""

import re
import time
from datetime import date, datetime, timedelta

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from sqlalchemy import Engine
from sqlmodel import Session, func, select

from app.models import OilPrices

_MONTH_MAP = {
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "May": 5,
    "Jun": 6,
    "Jul": 7,
    "Aug": 8,
    "Sep": 9,
    "Oct": 10,
    "Nov": 11,
    "Dec": 12,
}

DEFAULT_START_DATE = "2025-03-03"


def _parse_week_start(week_str: str) -> date | None:
    """Parse 'YYYY Mon-DD to Mon-DD' -> Monday date."""
    m = re.match(r"(\d{4})\s+(\w{3})-\s*(\d+)", week_str)
    if not m:
        return None
    year, mon_abbr, day = int(m.group(1)), m.group(2), int(m.group(3))
    month = _MONTH_MAP.get(mon_abbr)
    if not month:
        return None
    try:
        return date(year, month, day)
    except ValueError:
        return None


def _parse_eia_table(html: str) -> list[tuple[date, float]]:
    """Extract (date, price_usd) pairs from EIA daily table HTML."""
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.DOTALL)
    records: list[tuple[date, float]] = []
    for row in rows:
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
        if not cells:
            continue
        clean = [
            re.sub(r"<[^>]+>", "", c).strip().replace("\xa0", "").replace("&nbsp;", "")
            for c in cells
        ]
        if not clean or not re.search(r"\d{4}\s+\w{3}", clean[0]):
            continue
        week_start = _parse_week_start(clean[0])
        if not week_start:
            continue
        # Columns 1-5 = Mon..Fri
        for i, price_str in enumerate(clean[1:6]):
            if not price_str or price_str in ("-", "NA", "W", "--"):
                continue
            try:
                price = float(price_str)
            except ValueError:
                continue
            records.append((week_start + timedelta(days=i), price))
    return records


def scrape_oil_prices_direct(engine: Engine, start_date: str | None = None) -> None:
    """
    Scrapuje ceny ropy z EIA przez HTML table (bez pobierania pliku).
    """
    print(f"\n[OIL]  Starting oil prices scraping at {datetime.now()}")

    try:
        # Sprawdz ostatnia date w bazie
        with Session(engine) as session:
            latest_date_result = session.exec(select(func.max(OilPrices.data))).first()
            if latest_date_result:
                latest_date = latest_date_result.date()
                print(f"[OIL]  Latest date in DB: {latest_date}")
            else:
                latest_date = None
                print("[OIL]  No existing data in DB")

        # Konfiguracja Selenium (tylko do odczytu strony, bez pobierania)
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.binary_location = "/usr/bin/chromium"

        print("[OIL]  Starting browser...")
        driver = webdriver.Chrome(
            service=Service("/usr/bin/chromedriver"), options=chrome_options
        )

        try:
            url = "https://www.eia.gov/dnav/pet/hist/LeafHandler.ashx?n=pet&s=RBRTE&f=d"
            driver.get(url)
            print("[OIL] Loading EIA table...")
            time.sleep(6)
            page_html = driver.page_source
        finally:
            driver.quit()

        # Parsuj tabele HTML
        all_records = _parse_eia_table(page_html)
        print(f"[OIL]  Parsed {len(all_records)} total data points from EIA")

        if not all_records:
            print("[OIL]  No data parsed from EIA page")
            return

        # Filtruj tylko nowe rekordy
        effective_start_date = start_date or DEFAULT_START_DATE
        configured_start = date.fromisoformat(effective_start_date)
        print(f"[OIL]  Saving only records from: {effective_start_date}")

        new_records = [
            OilPrices(
                data=datetime.combine(d, datetime.min.time()),
                cena_usd=p,
                cena_pln=None,
                source="eia.gov",
            )
            for d, p in all_records
            if (latest_date is None or d > latest_date) and d >= configured_start
        ]

        if new_records:
            with Session(engine) as session:
                session.add_all(new_records)
                session.commit()
            print(f"[OIL] Saved {len(new_records)} new oil price records")
        else:
            print("[OIL] No new oil price data to save")

    except Exception as e:
        print(f"[OIL]  Critical error: {e}")
        import traceback

        traceback.print_exc()


def uzupelnij_oil_prices(engine: Engine, start_date: str | None = None) -> None:
    """
    Glowna funkcja do uzupelniania cen ropy - scrapuje bezposrednio z eia.gov
    """
    scrape_oil_prices_direct(engine, start_date=start_date)


if __name__ == "__main__":
    from sqlmodel import create_engine

    from app.core.config import settings

    engine = create_engine(str(settings.SQLALCHEMY_DATABASE_URI))
    scrape_oil_prices_direct(engine)
