import os, time, shutil, threading, csv
import pandas as pd
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
import itertools

# === Paths ===
DOWNLOAD_DIR = Path(os.getcwd()) / "uspa_downloads"
CHECKPOINT_FILE = Path(os.getcwd()) / "uspa_checkpoint.txt"
OUTPUT_FILE = Path(os.getcwd()) / "uspa_all_records.csv"
DOWNLOAD_DIR.mkdir(exist_ok=True)

WORKER_COUNT = 12  # number of parallel Chrome instances

# === USPA weight classes (kg portion only, matching CSV format before the "/") ===
MENS_WEIGHT_CLASSES = [
    "52kg", "56kg", "60kg", "67.5kg", "75kg", "82.5kg",
    "90kg", "100kg", "110kg", "125kg", "140kg", "140+kg",
]
WOMENS_WEIGHT_CLASSES = [
    "44kg", "48kg", "52kg", "56kg", "60kg", "67.5kg",
    "75kg", "82.5kg", "90kg", "100kg", "110kg", "110+kg",
]

# === All recognised USPA divisions (uppercase, matching CSV/DB format) ===
ALL_DIVISIONS_MEN = [
    "OPEN MEN",
    "JUNIOR MEN 13 TO 15", "JUNIOR MEN 16 TO 17", "JUNIOR MEN 18 TO 19", "JUNIOR MEN 20 TO 23",
    "SUBMASTER MEN 35 TO 39",
    "MASTER MEN 40 TO 44", "MASTER MEN 45 TO 49", "MASTER MEN 50 TO 54", "MASTER MEN 55 TO 59",
    "MASTER MEN 60 TO 64", "MASTER MEN 65 TO 69", "MASTER MEN 70 TO 74", "MASTER MEN 75 TO 79",
    "MASTER MEN 80 TO 84",
]
ALL_DIVISIONS_WOMEN = [
    "OPEN WOMEN",
    "JUNIOR WOMEN 13 TO 15", "JUNIOR WOMEN 16 TO 17", "JUNIOR WOMEN 18 TO 19", "JUNIOR WOMEN 20 TO 23",
    "SUBMASTER WOMEN 35 TO 39",
    "MASTER WOMEN 40 TO 44", "MASTER WOMEN 45 TO 49", "MASTER WOMEN 50 TO 54", "MASTER WOMEN 55 TO 59",
    "MASTER WOMEN 60 TO 64", "MASTER WOMEN 65 TO 69", "MASTER WOMEN 70 TO 74", "MASTER WOMEN 75 TO 79",
    "MASTER WOMEN 80 TO 84",
]
ALL_DIVISIONS = ALL_DIVISIONS_MEN + ALL_DIVISIONS_WOMEN

# === Configure Chrome ===
# Resolve ChromeDriver once so parallel threads don't race to download it
DRIVER_PATH = ChromeDriverManager().install()

def make_driver(download_dir: Path) -> webdriver.Chrome:
    opts = Options()
    opts.add_argument("--headless")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("window-size=1920,1080")
    prefs = {
        "download.default_directory": str(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    opts.add_experimental_option("prefs", prefs)
    return webdriver.Chrome(service=Service(DRIVER_PATH), options=opts)

# Thread-local storage: each worker thread gets its own driver + download dir
thread_local = threading.local()
all_drivers: list[webdriver.Chrome] = []
drivers_lock = threading.Lock()

def get_driver() -> tuple[webdriver.Chrome, Path]:
    if not hasattr(thread_local, "driver"):
        name = threading.current_thread().name.replace("/", "-")
        worker_dir = DOWNLOAD_DIR / name
        worker_dir.mkdir(exist_ok=True)
        thread_local.driver = make_driver(worker_dir)
        thread_local.download_dir = worker_dir
        with drivers_lock:
            all_drivers.append(thread_local.driver)
    return thread_local.driver, thread_local.download_dir

# === Generate all record page URLs ===
states = [
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado", "connecticut", "delaware", "florida",
    "georgia", "hawaii", "idaho", "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana", "maine",
    "maryland", "massachusetts", "michigan", "minnesota", "mississippi", "missouri", "montana", "nebraska",
    "nevada", "new-hampshire", "new-jersey", "new-mexico", "new-york", "north-carolina", "north-dakota", "ohio",
    "oklahoma", "oregon", "pennsylvania", "rhode-island", "south-carolina", "south-dakota", "tennessee", "texas",
    "utah", "vermont", "virginia", "washington", "west-virginia", "wisconsin", "wyoming"
]
special_locations = ["national", "ipl-world"]
locations = states + special_locations
events = [
    "raw-powerlifting", "classic-powerlifting", "raw-bench-only", "raw-deadlift-only",
    "single-ply-powerlifting", "single-ply-bench-only", "single-ply-deadlift-only",
    "multi-ply-powerlifting", "multi-ply-bench-only", "multi-ply-deadlift-only"
]

statuses = ["drug-tested", "non-tested"]

base_url = "https://records.uspa.net/records.php"
urls = [
    f"{base_url}?location={location}&status={status}&event={event}"
    for location, status, event in itertools.product(locations, statuses, events)
]

print(f"\nTotal record pages to check: {len(urls)}")

# === Load checkpoint (skip already-completed URLs) ===
completed_urls = set()
if CHECKPOINT_FILE.exists():
    completed_urls = set(CHECKPOINT_FILE.read_text().splitlines())
    print(f"Resuming — {len(completed_urls)} URLs already completed.")

remaining_urls = [u for u in urls if u not in completed_urls]
print(f"URLs remaining: {len(remaining_urls)}")


def wait_for_download(download_dir: Path, timeout: int = 15) -> Path | None:
    """Wait until a CSV is fully downloaded (no .crdownload temp file remains)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        crdownloads = list(download_dir.glob("*.crdownload"))
        csvs = list(download_dir.glob("*.csv"))
        if csvs and not crdownloads:
            return csvs[0]
        time.sleep(0.5)
    return None


def clear_downloads(download_dir: Path) -> None:
    for f in download_dir.glob("*.csv"):
        f.unlink()
    for f in download_dir.glob("*.crdownload"):
        f.unlink()


LIFT_ORDER = {"Squat": 0, "Bench": 1, "Deadlift": 2, "TOTAL": 3}


def _extract_kg(weight_class: str) -> str:
    """Extract the kg portion from CSV format: '60kg/132.2lb' → '60kg', '140+kg/SHW' → '140+kg'."""
    return str(weight_class).split("/")[0].strip()


def _wc_sort_key(weight_class: str) -> float:
    """Numeric sort key for a weight class: '67.5kg/...' → 67.5, '140+kg/...' → 140.1."""
    kg = _extract_kg(str(weight_class)).replace("kg", "")
    if kg.endswith("+"):
        return float(kg[:-1]) + 0.1
    try:
        return float(kg)
    except ValueError:
        return 9999.0


def _expected_weight_classes(division: str) -> list[str]:
    """Return the expected USPA weight class list based on gender in the division name."""
    div = str(division).upper()
    if "WOMEN" in div:
        return WOMENS_WEIGHT_CLASSES
    if "MEN" in div:
        return MENS_WEIGHT_CLASSES
    return sorted(set(MENS_WEIGHT_CLASSES + WOMENS_WEIGHT_CLASSES))


def _expected_lifts(event: str) -> list[str]:
    """Return the lifts that make sense for the given event type."""
    e = event.lower()
    if "bench-only" in e:
        return ["Bench"]
    if "deadlift-only" in e:
        return ["Deadlift"]
    return ["Squat", "Bench", "Deadlift", "TOTAL"]


def fill_all_vacancies(
    df: pd.DataFrame, location: str, event: str, status: str
) -> pd.DataFrame:
    """Ensure every USPA division × weight class × lift has a row for this combination.

    Fills three levels of gaps:
      1. Missing weight classes within existing division+lift groups
      2. Missing lifts within existing divisions
      3. Entirely missing divisions
    Any division that appears in the CSV but is not in ALL_DIVISIONS is also
    handled so unknown/future divisions don't lose their weight-class coverage.
    """
    lifts = _expected_lifts(event)

    # Build a set of (division_upper, lift, wc_kg) for all real records
    real = df[df["HasRecord"] == True]
    existing = set(
        zip(
            real["Division"].str.upper(),
            real["Lift"].astype(str),
            real["Weight Class"].apply(lambda x: _extract_kg(str(x))),
        )
    )

    placeholder_rows = []

    def _add_if_missing(division: str, lift: str, wc: str) -> None:
        if (division.upper(), lift, wc) not in existing:
            placeholder_rows.append({
                "Division": division,
                "Weight Class": wc,
                "Lift": lift,
                "Name": "No existing record",
                "Kilos": float("nan"),
                "Pounds": float("nan"),
                "Date": None,
                "Location": location,
                "Event": event,
                "Status": status,
                "HasRecord": False,
            })

    # Fill all known USPA divisions
    for division in ALL_DIVISIONS:
        for lift in lifts:
            for wc in _expected_weight_classes(division):
                _add_if_missing(division, lift, wc)

    # Also fill any divisions in the CSV not covered by ALL_DIVISIONS
    known_upper = {d.upper() for d in ALL_DIVISIONS}
    for division in real["Division"].dropna().unique():
        if str(division).upper() not in known_upper:
            for lift in lifts:
                for wc in _expected_weight_classes(division):
                    _add_if_missing(division, lift, wc)

    combined = pd.concat(
        [df, pd.DataFrame(placeholder_rows)] if placeholder_rows else [df],
        ignore_index=True,
    )

    # Sort: Division → Weight Class (numeric kg) → Lift
    combined["_wc_sort"] = combined["Weight Class"].apply(_wc_sort_key)
    combined["_lift_sort"] = combined["Lift"].map(LIFT_ORDER).fillna(99)
    combined = (
        combined
        .sort_values(["Division", "_wc_sort", "_lift_sort"], ignore_index=True)
        .drop(columns=["_wc_sort", "_lift_sort"])
    )
    return combined


def _read_csv_robust(csv_path: Path) -> pd.DataFrame | None:
    """Read a USPA CSV, reconstructing names that contain unquoted commas.

    The CSV always has exactly 7 logical columns:
        Division, Weight Class, Lift, Name, Kilos, Pounds, Date
    When a name contains a comma (e.g. "Smith, John"), the raw row has 8+
    fields.  We recover the full name by joining everything between the 3rd
    and the last-3 positions.
    """
    COLS = ["Division", "Weight Class", "Lift", "Name", "Kilos", "Pounds", "Date"]
    rows = []
    try:
        with open(csv_path, newline="", encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f)
            try:
                next(reader)  # skip header
            except StopIteration:
                return None  # empty file
            for raw in reader:
                if len(raw) == 0:
                    continue
                if len(raw) == 7:
                    rows.append(raw)
                elif len(raw) > 7:
                    # Reconstruct: first 3 cols fixed, last 3 cols fixed, name is everything in between
                    name = ", ".join(raw[3:-3]).strip()
                    rows.append([raw[0], raw[1], raw[2], name, raw[-3], raw[-2], raw[-1]])
                # rows with < 7 fields are silently dropped (malformed)
    except OSError:
        return None

    if not rows:
        return None

    df = pd.DataFrame(rows, columns=COLS)
    # Coerce numeric columns
    df["Kilos"] = pd.to_numeric(df["Kilos"], errors="coerce")
    df["Pounds"] = pd.to_numeric(df["Pounds"], errors="coerce")
    return df


def scrape_url(driver, url: str, download_dir: Path) -> pd.DataFrame | None:
    """Visit a single record page, download its CSV, and return a DataFrame.

    Returns:
        DataFrame with records (HasRecord=True), or a single placeholder row
        (HasRecord=False) when no records exist for the combination.
        Returns None only on a page-level error (navigation/iframe failure).
    """
    params = parse_qs(urlparse(url).query)
    location_value = params["location"][0]
    status_value = params["status"][0]
    event_value = params.get("event", [""])[0]

    def no_record_row() -> pd.DataFrame:
        return pd.DataFrame([{
            "Division": None, "Weight Class": None, "Lift": None,
            "Name": None, "Kilos": float("nan"), "Pounds": float("nan"), "Date": None,
            "Location": location_value, "Event": event_value, "Status": status_value,
            "HasRecord": False,
        }])

    driver.get(url)

    # Wait for iframe and switch into it
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "content-iframe")))
    driver.switch_to.frame(driver.find_element(By.ID, "content-iframe"))

    try:
        clear_downloads(download_dir)

        try:
            button = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//button[text()='Download CSV']"))
            )
        except TimeoutException:
            print("  No records for this combination")
            return no_record_row()

        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
        WebDriverWait(driver, 5).until(EC.element_to_be_clickable(button)).click()
        print("  Download initiated")

        csv_path = wait_for_download(download_dir)
        if csv_path is None:
            print("  CSV download timed out")
            return no_record_row()

        df = _read_csv_robust(csv_path)
        if df is None:
            print("  CSV was empty — no records")
            return no_record_row()
        df["Location"] = location_value
        df["Event"] = event_value
        df["Status"] = status_value
        df["HasRecord"] = True

        if df.empty:
            print("  CSV was empty — no records")
            return no_record_row()

        df = fill_all_vacancies(df, location_value, event_value, status_value)
        added = len(df[df["HasRecord"] == False])
        print(f"  Loaded {len(df[df['HasRecord'] == True])} rows ({added} vacancies filled)")
        return df

    except Exception as e:
        print(f"  Failed: {e}")
        safe_name = f"{location_value}_{status_value}_{event_value}".replace("/", "-")
        driver.save_screenshot(f"debug_{safe_name}.png")
        return None

    finally:
        driver.switch_to.default_content()


# === Main scrape loop ===
lock = threading.Lock()

# On resume, the existing OUTPUT_FILE already contains prior rows — no need to reload it.
if OUTPUT_FILE.exists() and completed_urls:
    prior_rows = sum(1 for _ in open(OUTPUT_FILE)) - 1  # subtract header
    print(f"Resuming — {prior_rows} rows already in output file.")

total = len(remaining_urls)
rows_written = 0

def process_url(args: tuple) -> None:
    global rows_written
    i, url = args
    driver, download_dir = get_driver()
    print(f"\n[{i}/{total}] {url}")
    try:
        df = scrape_url(driver, url, download_dir)
    except Exception as e:
        print(f"  Error loading page: {e}")
        df = None
    with lock:
        if df is not None:
            # Append-only: write header only when creating the file for the first time
            write_header = not OUTPUT_FILE.exists()
            df.to_csv(OUTPUT_FILE, mode="a", header=write_header, index=False)
            rows_written += len(df)
        with open(CHECKPOINT_FILE, "a") as f:
            f.write(url + "\n")

try:
    args = [(i, url) for i, url in enumerate(remaining_urls, 1)]
    with ThreadPoolExecutor(max_workers=WORKER_COUNT) as executor:
        futures = [executor.submit(process_url, a) for a in args]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"  Worker error: {e}")
finally:
    for d in all_drivers:
        try:
            d.quit()
        except Exception:
            pass

# === Final output ===
if OUTPUT_FILE.exists():
    print(f"\nAll records saved to: {OUTPUT_FILE}  ({rows_written} new rows written this run)")
    CHECKPOINT_FILE.unlink(missing_ok=True)
else:
    print("\nNo CSVs were collected.")

# === Cleanup download folder ===
shutil.rmtree(DOWNLOAD_DIR, ignore_errors=True)
