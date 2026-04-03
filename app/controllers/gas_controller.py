"""
Gas prices data controller - direct scraping
"""

import pandas as pd
from datetime import datetime, timedelta
from sqlmodel import Session, select
from sqlalchemy import Engine
from app.models import GasPrices
import requests
import os
import time
import tempfile
import shutil
import importlib


DEFAULT_START_DATE = "2025-03-03"


def _download_gas_data_from_api() -> pd.DataFrame:
    api_url = "https://energy-api.instrat.pl/api/prices/gas_price_rdn_daily?all=1"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://energy.instrat.pl/ceny/gaz-rdn/",
        "Origin": "https://energy.instrat.pl",
        "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
    }
    response = requests.get(api_url, timeout=60, headers=headers)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise ValueError("Unexpected API payload format")
    return pd.DataFrame(payload)


def _download_gas_data_from_selenium_csv() -> pd.DataFrame:
    # Importy dynamiczne, żeby fallback nie wymagał Selenium przy działającym API.
    webdriver = importlib.import_module("selenium.webdriver")
    Service = importlib.import_module("selenium.webdriver.chrome.service").Service
    Options = importlib.import_module("selenium.webdriver.chrome.options").Options
    By = importlib.import_module("selenium.webdriver.common.by").By
    WebDriverWait = importlib.import_module("selenium.webdriver.support.ui").WebDriverWait
    EC = importlib.import_module("selenium.webdriver.support.expected_conditions")

    download_dir = tempfile.mkdtemp()
    driver = None
    downloaded_file = None

    try:
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.binary_location = "/usr/bin/chromium"

        prefs = {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        }
        chrome_options.add_experimental_option("prefs", prefs)

        service = Service("/usr/bin/chromedriver")
        driver = webdriver.Chrome(service=service, options=chrome_options)
        wait = WebDriverWait(driver, 20)

        driver.get("https://energy.instrat.pl/ceny/gaz-rdn/")

        try:
            cookie_btn = wait.until(
                EC.element_to_be_clickable(
                    (
                        By.XPATH,
                        "//button[contains(text(), 'Zgadzam') or contains(text(), 'Akceptuj')]",
                    )
                )
            )
            cookie_btn.click()
            time.sleep(1)
        except Exception:
            pass

        download_btn = wait.until(
            EC.element_to_be_clickable(
                (
                    By.XPATH,
                    "//a[contains(@class, 'elementor-button') and .//span[contains(text(), 'Wszystkie')] ]",
                )
            )
        )
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", download_btn)
        time.sleep(1)
        driver.execute_script("arguments[0].click();", download_btn)

        start_time = time.time()
        while time.time() - start_time < 40:
            files = os.listdir(download_dir)
            csv_files = [f for f in files if f.endswith(".csv") and "crdownload" not in f]
            if csv_files:
                csv_files = sorted(csv_files)
                downloaded_file = os.path.join(download_dir, csv_files[-1])
                break
            time.sleep(1)

        if not downloaded_file:
            raise TimeoutError("CSV download timeout")

        return pd.read_csv(downloaded_file)

    finally:
        if driver:
            driver.quit()
        shutil.rmtree(download_dir, ignore_errors=True)


def scrape_gas_prices_direct(engine: Engine, start_date: str | None = None) -> None:
    """
    Scrapuje ceny gazu bezpośrednio z energy.instrat.pl i zapisuje do bazy danych.
    """
    print(f"\n[GAS]  Starting gas prices scraping at {datetime.now()}")
    
    try:
        is_api_data = True
        print("[GAS] Downloading data from API...")
        try:
            df = _download_gas_data_from_api()
        except requests.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else None
            if status_code == 403:
                print("[GAS]  API 403 Forbidden - fallback to Selenium CSV...")
                df = _download_gas_data_from_selenium_csv()
                is_api_data = False
            else:
                raise

        print("[GAS]  Processing downloaded data...")
        print(f"[GAS] Columns: {df.columns.tolist()}")
        print(f"[GAS]  Loaded {len(df)} rows")
        
        # Oczekiwane nagłówki: date, indeks, price, volume
        normalized_cols = {col.strip().lower(): col for col in df.columns}
        required_headers = ["date", "indeks", "price", "volume"]
        missing_headers = [h for h in required_headers if h not in normalized_cols]

        if missing_headers:
            print(f"[GAS]  Missing required CSV headers: {missing_headers}")
            return

        date_col = normalized_cols["date"]
        indeks_col = normalized_cols["indeks"]
        price_col = normalized_cols["price"]
        volume_col = normalized_cols["volume"]

        # Parsowanie i normalizacja danych
        # API: ISO UTC. CSV: format dzienny typu DD.MM.YYYY.
        if is_api_data:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce", utc=True).dt.tz_convert(None)
        else:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce", dayfirst=True)
        df[price_col] = pd.to_numeric(df[price_col], errors="coerce")
        df[volume_col] = pd.to_numeric(df[volume_col], errors="coerce")
        df[indeks_col] = df[indeks_col].astype(str).str.strip()

        df = df.dropna(subset=[date_col, price_col])
        df = df[df[indeks_col] != ""]

        if df.empty:
            print("[GAS] ℹ No valid rows after CSV normalization")
            return

        # Dla tej tabeli trzymamy stałe source jak dotychczas.
        # (API zwraca obecnie indeks tgegasda.)
        df["source_key"] = "energy.instrat.pl"

        source_keys = df["source_key"].unique().tolist()

        # Pobierz już zapisane daty osobno dla każdej serii (source).
        # To naprawia sytuację, gdy historycznie były błędne daty "w przyszłości"
        # i filtr max(data) blokował zapisywanie poprawnych nowych rekordów.
        today_plus_one = datetime.now().date() + timedelta(days=1)
        with Session(engine) as session:
            existing_rows = session.exec(
                select(GasPrices.source, GasPrices.data)
                .where(GasPrices.source.in_(source_keys))
            ).all()

        existing_keys = {
            (source, data_dt.date())
            for source, data_dt in existing_rows
            if data_dt is not None and data_dt.date() <= today_plus_one
        }
        print(f"[GAS]  Existing records loaded: {len(existing_keys)}")
        
        gas_records = []

        effective_start_date = start_date or DEFAULT_START_DATE
        start_cutoff = datetime.fromisoformat(effective_start_date).date()
        print(f"[GAS] Saving only records from: {effective_start_date}")

        seen_keys = set()

        for _, row in df.sort_values(by=date_col).iterrows():
            try:
                data_date = pd.Timestamp(row[date_col]).to_pydatetime()
                source_key = row["source_key"]

                if data_date.date() < start_cutoff:
                    continue

                record_key = (data_date, source_key)
                if record_key in seen_keys:
                    continue
                seen_keys.add(record_key)

                if (source_key, data_date.date()) in existing_keys:
                    continue

                cena_eur = float(row[price_col]) if pd.notna(row[price_col]) else None
                volume = float(row[volume_col]) if pd.notna(row[volume_col]) else None
                
                gas_record = GasPrices(
                    data=data_date,
                    cena_pln=None,
                    cena_eur=cena_eur,
                    volume=volume,
                    source=source_key
                )
                
                gas_records.append(gas_record)
                
            except Exception as e:
                print(f"[GAS]  Error processing row: {e}")
                continue
        
        # Zapisz do bazy
        if gas_records:
            with Session(engine) as session:
                session.add_all(gas_records)
                session.commit()
                
            print(f"[GAS] Saved {len(gas_records)} new gas price records")
        else:
            print("[GAS] ℹ No new gas price data to save")
        
    except Exception as e:
        print(f"[GAS]  Critical error: {e}")
        import traceback
        traceback.print_exc()


def uzupelnij_gas_prices(engine: Engine, start_date: str | None = None) -> None:
    """
    Główna funkcja do uzupełniania cen gazu - scrapuje bezpośrednio z internetu
    """
    scrape_gas_prices_direct(engine, start_date=start_date)


if __name__ == "__main__":
    from app.core.config import settings
    from sqlmodel import create_engine
    
    engine = create_engine(str(settings.SQLALCHEMY_DATABASE_URI))
    scrape_gas_prices_direct(engine)