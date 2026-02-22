import os, time, shutil
import pandas as pd
from pathlib import Path
from urllib.parse import urlparse, parse_qs
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

# === USPA weight classes (kg portion only, matching CSV format before the "/") ===
MENS_WEIGHT_CLASSES = [
    "52kg", "56kg", "60kg", "67.5kg", "75kg", "82.5kg",
    "90kg", "100kg", "110kg", "125kg", "140kg", "140+kg",
]
WOMENS_WEIGHT_CLASSES = [
    "44kg", "48kg", "52kg", "56kg", "60kg", "67.5kg",
    "75kg", "82.5kg", "90kg", "100kg", "110kg", "110+kg",
]

# === Configure Chrome ===
options = Options()
options.add_argument("--headless")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("window-size=1920,1080")
prefs = {
    "download.default_directory": str(DOWNLOAD_DIR),
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
}
options.add_experimental_option("prefs", prefs)

# === Generate all record page URLs ===
# --- TEST MODE: only Ohio + raw-powerlifting ---
# To run the full scrape, comment out the test block below and uncomment the full lists.

# -- FULL lists (re-enable for production) --
# states = [
#     "alabama", "alaska", "arizona", "arkansas", "california", "colorado", "connecticut", "delaware", "florida",
#     "georgia", "hawaii", "idaho", "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana", "maine",
#     "maryland", "massachusetts", "michigan", "minnesota", "mississippi", "missouri", "montana", "nebraska",
#     "nevada", "new-hampshire", "new-jersey", "new-mexico", "new-york", "north-carolina", "north-dakota", "ohio",
#     "oklahoma", "oregon", "pennsylvania", "rhode-island", "south-carolina", "south-dakota", "tennessee", "texas",
#     "utah", "vermont", "virginia", "washington", "west-virginia", "wisconsin", "wyoming"
# ]
# special_locations = ["national", "ipl-world"]
# locations = states + special_locations
# events = [
#     "raw-powerlifting", "classic-powerlifting", "raw-bench-only", "raw-deadlift-only",
#     "single-ply-powerlifting", "single-ply-bench-only", "single-ply-deadlift-only",
#     "multi-ply-powerlifting", "multi-ply-bench-only", "multi-ply-deadlift-only"
# ]

# -- TEST values --
locations = ["ohio"]
events = ["raw-powerlifting"]

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


def fill_missing_weight_classes(
    df: pd.DataFrame, location: str, event: str, status: str
) -> pd.DataFrame:
    """For each Division+Lift group, add placeholder rows for any missing USPA weight classes."""
    placeholder_rows = []

    for (division, lift), group in df.groupby(["Division", "Lift"]):
        expected = _expected_weight_classes(division)
        present = {_extract_kg(wc) for wc in group["Weight Class"].dropna()}
        for wc in expected:
            if wc not in present:
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

    combined = pd.concat(
        [df, pd.DataFrame(placeholder_rows)] if placeholder_rows else [df],
        ignore_index=True,
    )

    # Sort into logical order: Division → Weight Class (numeric kg) → Lift
    combined["_wc_sort"] = combined["Weight Class"].apply(_wc_sort_key)
    combined["_lift_sort"] = combined["Lift"].map(LIFT_ORDER).fillna(99)
    combined = (
        combined
        .sort_values(["Division", "_wc_sort", "_lift_sort"], ignore_index=True)
        .drop(columns=["_wc_sort", "_lift_sort"])
    )
    return combined


def scrape_url(driver, url: str) -> pd.DataFrame | None:
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
        clear_downloads(DOWNLOAD_DIR)

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

        csv_path = wait_for_download(DOWNLOAD_DIR)
        if csv_path is None:
            print("  CSV download timed out")
            return no_record_row()

        df = pd.read_csv(csv_path, header=0)
        df = df.iloc[:, :7]
        df.columns = ["Division", "Weight Class", "Lift", "Name", "Kilos", "Pounds", "Date"]
        df["Location"] = location_value
        df["Event"] = event_value
        df["Status"] = status_value
        df["HasRecord"] = True

        if df.empty:
            print("  CSV was empty — no records")
            return no_record_row()

        df = fill_missing_weight_classes(df, location_value, event_value, status_value)
        added = len(df[df["HasRecord"] == False])
        print(f"  Loaded {len(df[df['HasRecord'] == True])} rows ({added} missing weight classes added)")
        return df

    except Exception as e:
        print(f"  Failed: {e}")
        safe_name = f"{location_value}_{status_value}_{event_value}".replace("/", "-")
        driver.save_screenshot(f"debug_{safe_name}.png")
        return None

    finally:
        driver.switch_to.default_content()


# === Main scrape loop ===
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
all_data = []

# Load any rows already saved from a previous run
if OUTPUT_FILE.exists() and completed_urls:
    all_data.append(pd.read_csv(OUTPUT_FILE))
    print(f"Loaded {len(all_data[0])} existing rows from previous run.")

try:
    for i, url in enumerate(remaining_urls, 1):
        print(f"\n[{i}/{len(remaining_urls)}] {url}")
        try:
            df = scrape_url(driver, url)
            if df is not None:
                all_data.append(df)
                # Save incrementally after each page (records or no-record placeholder)
                pd.concat(all_data, ignore_index=True).to_csv(OUTPUT_FILE, index=False)
            # Mark URL as completed regardless (even if no data, to avoid retrying broken pages)
            with open(CHECKPOINT_FILE, "a") as f:
                f.write(url + "\n")
        except Exception as e:
            print(f"  Error loading page: {e}")
            continue
finally:
    driver.quit()

# === Final output ===
if all_data:
    combined_df = pd.concat(all_data, ignore_index=True)
    combined_df.to_csv(OUTPUT_FILE, index=False)
    print(f"\nAll records saved to: {OUTPUT_FILE}  ({len(combined_df)} total rows)")
    # Clear checkpoint on successful full completion
    if not remaining_urls or len(remaining_urls) == len([u for u in urls if u not in completed_urls]):
        CHECKPOINT_FILE.unlink(missing_ok=True)
else:
    print("\nNo CSVs were collected.")

# === Cleanup download folder ===
shutil.rmtree(DOWNLOAD_DIR, ignore_errors=True)
