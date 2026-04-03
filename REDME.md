# Inzynierka – pobieranie danych i scheduler

W niniejszym dokumencie przedstawiono procedurę:
- uruchomienia usług,
- ręcznego pobrania danych przez API,
- sterowania schedulerem `fetch_all` (włączenie/wyłączenie),
- uruchamiania pobierania od wskazanej daty `start_date`.

## 1) Uruchomienie projektu

Należy utworzyć plik `.env` (np. przez skopiowanie `.env.example`).

Następnie należy uruchomić podstawowe usługi:

```bash
docker compose build
docker compose up -d
```

Najważniejsze serwisy:
- `app` – API FastAPI (port `8000`),
- `db` – PostgreSQL/Timescale (port `5432`),
- `fetch_scheduler` – automatyczne wywoływanie `fetch-all`.

Weryfikacja statusu kontenerów:

```bash
docker compose ps
```

## 2) Ręczne pobieranie danych (`fetch-all`)

### 2.1. Wywołanie standardowe

```bash
curl -X GET "http://localhost:8000/api/v1/data/fetch-all"
```

### 2.2. Wywołanie od wskazanej daty

W celu ograniczenia zakresu pobierania można przekazać parametr `start_date`.

- domyślna wartość `start_date`: `2026-01-01`,
- wymagany format: `YYYY-MM-DD`.

Przykład:

```bash
curl -X GET "http://localhost:8000/api/v1/data/fetch-all?start_date=2026-03-25"
```

## 3) Status pobranych danych

```bash
curl -X GET "http://localhost:8000/api/v1/data/fetch-all/status"
```

Endpoint zwraca m.in. ostatnie daty danych dla źródeł (np. `energy_prices`, `demand_kse`).

## 4) Sterowanie schedulerem (ON/OFF)

Scheduler cyklicznie wywołuje `fetch-all` co 15 minut.

### 4.1. Włączenie schedulera

```bash
curl -X POST "http://localhost:8000/api/v1/data/fetch-all/scheduler/enable"
```

### 4.2. Wyłączenie schedulera

```bash
curl -X POST "http://localhost:8000/api/v1/data/fetch-all/scheduler/disable"
```

### 4.3. Sprawdzenie stanu schedulera

```bash
curl -X GET "http://localhost:8000/api/v1/data/fetch-all/scheduler/status"
```

## 5) Pobrane wartości mozna obejrzec uzywajac Grafany/Adminera

Domyslne dane do logowania: 

ADMINER:
POSTGRES_SERVER=localhost
POSTGRES_PORT=5432
POSTGRES_DB=app
POSTGRES_USER=postgres
POSTGRES_PASSWORD=app

GRAFANA: 
GF_SECURITY_ADMIN_USER=admin
GF_SECURITY_ADMIN_PASSWORD=admin

Logi API:

```bash
docker compose logs -f app
```

Logi schedulera:

```bash
docker compose logs -f fetch_scheduler
```