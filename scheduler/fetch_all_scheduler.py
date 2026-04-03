#!/usr/bin/env python3
"""
Scheduler dla automatycznego uruchamiania fetch_all co 5 minut
"""

import logging
import signal
import sys

import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

# Konfiguracja logowania
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("/app/fetch_scheduler.log"), logging.StreamHandler()],
)

logger = logging.getLogger(__name__)

# Konfiguracja
API_BASE_URL = "http://app:8000"
FETCH_ALL_ENDPOINT = f"{API_BASE_URL}/api/v1/data/fetch-all"
STATUS_ENDPOINT = f"{API_BASE_URL}/api/v1/data/fetch-all/status"
SCHEDULER_CONTROL_ENDPOINT = f"{API_BASE_URL}/api/v1/data/fetch-all/scheduler/status"


def check_api_health():
    """Sprawdza czy API jest dostępne"""
    try:
        response = requests.get(STATUS_ENDPOINT, timeout=10)
        return response.status_code == 200
    except Exception as e:
        logger.error(f" API health check failed: {e}")
        return False


def run_fetch_all():
    """Uruchamia fetch_all endpoint"""
    try:
        logger.info(" Starting automatic fetch_all process...")

        # Sprawdź czy scheduler jest włączony po stronie API
        if not is_scheduler_enabled():
            logger.info(" Scheduler is disabled in API settings, skipping this run")
            return

        # Sprawdź czy API jest dostępne
        if not check_api_health():
            logger.error(" API is not available, skipping fetch_all")
            return

        # Wywołaj fetch_all endpoint
        response = requests.get(FETCH_ALL_ENDPOINT, timeout=30)

        if response.status_code == 200:
            data = response.json()
            logger.info("Fetch_all started successfully")
            logger.info(f"Data sources: {data.get('data_sources', [])}")
            logger.info(f"Started at: {data.get('timestamp', 'unknown')}")
        else:
            logger.error(
                f" fetch_all failed with status {response.status_code}: {response.text}"
            )

    except requests.exceptions.RequestException as e:
        logger.error(f" Network error calling fetch_all: {e}")
    except Exception as e:
        logger.error(f" Unexpected error in run_fetch_all: {e}")


def check_fetch_status():
    """Sprawdza status ostatniego fetch_all (opcjonalnie)"""
    try:
        response = requests.get(STATUS_ENDPOINT, timeout=10)
        if response.status_code == 200:
            data = response.json()
            logger.info(f" Fetch status: {data.get('status', 'unknown')}")
            return data
        else:
            logger.warning(f" Could not get fetch status: {response.status_code}")
            return None
    except Exception as e:
        logger.warning(f" Error checking fetch status: {e}")
        return None


def is_scheduler_enabled() -> bool:
    """Sprawdza flagę włączenia schedulera po stronie API."""
    try:
        response = requests.get(SCHEDULER_CONTROL_ENDPOINT, timeout=10)
        if response.status_code != 200:
            logger.warning(
                " Could not read scheduler status from API (%s). Assuming enabled.",
                response.status_code,
            )
            return True

        data = response.json()
        return bool(data.get("scheduler_enabled", True))
    except Exception as e:
        logger.warning(f" Error reading scheduler status: {e}. Assuming enabled.")
        return True


def signal_handler(signum, frame):
    """Obsługuje sygnały zakończenia"""
    logger.info(" Otrzymano sygnał zakończenia, zamykam scheduler...")
    sys.exit(0)


def main():
    """Główna funkcja schedulera"""
    logger.info(" Uruchamiam fetch_all scheduler (co 5 minut)...")

    # Rejestruj obsługę sygnałów
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Sprawdź połączenie z API przy starcie (z retry, bo API może jeszcze wstawać)
    logger.info(" Oczekuję na uruchomienie API...")
    import time

    api_ready = False
    for i in range(15):
        if check_api_health():
            api_ready = True
            break
        logger.info(
            f"API jeszcze nie odpowiada. Próba {i + 1}/15... Ponawiam za 2s."
        )
        time.sleep(2)

    if not api_ready:
        logger.error(
            "Nie można połączyć się z API po wielu próbach. Sprawdź czy aplikacja działa."
        )
        sys.exit(1)

    logger.info("Połączenie z API OK")

    # Uruchom fetch_all od razu przy starcie (opcjonalnie)
    logger.info("    Uruchamiam pierwszego fetch_all przy starcie...")
    run_fetch_all()

    # Konfiguruj scheduler
    scheduler = BlockingScheduler()

    # Dodaj zadanie fetch_all co 15 minut
    scheduler.add_job(
        run_fetch_all,
        IntervalTrigger(minutes=15),
        id="fetch_all_job",
        name="Automatic fetch_all every 15 minutes",
        replace_existing=True,
        max_instances=1,  # Tylko jedna instancja na raz
    )

    # Opcjonalnie: sprawdzanie statusu co 15 minut
    scheduler.add_job(
        check_fetch_status,
        IntervalTrigger(minutes=15),
        id="status_check_job",
        name="Check fetch status every 15 minutes",
        replace_existing=True,
    )

    logger.info(" Scheduler skonfigurowany:")
    logger.info("   - fetch_all: co 15 minut")
    logger.info("   - status check: co 15 minut")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info(" Scheduler zatrzymany przez użytkownika")
    except Exception as e:
        logger.error(f"Błąd schedulera: {e}")


if __name__ == "__main__":
    main()
