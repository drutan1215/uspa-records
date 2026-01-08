import os, time, glob, shutil
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import itertools

# === Setup download directory ===
DOWNLOAD_DIR = os.path.join(os.getcwd(), "uspa_downloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# === Configure Chrome ===
options = Options()
options.add_argument("--headless")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("window-size=1920,1080")
prefs = {
    "download.default_directory": DOWNLOAD_DIR,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
}
options.add_experimental_option("prefs", prefs)
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

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

statuses = ["drug-tested", "non-tested"]
events = [
    "raw-powerlifting", "classic-powerlifting", "raw-bench-only", "raw-deadlift-only",
    "single-ply-powerlifting", "single-ply-bench-only", "single-ply-deadlift-only",
    "multi-ply-powerlifting", "multi-ply-bench-only", "multi-ply-deadlift-only"
]

base_url = "https://records.uspa.net/records.php"
urls = [
    f"{base_url}?location={location}&status={status}&event={event}"
    for location, status, event in itertools.product(locations, statuses, events)
]

print(f"\n🔗 Total record pages to check: {len(urls)}")

all_data = []

# === Loop through record pages ===
for url in urls:
    print(f"\n🔍 Visiting: {url}")
    try:
        driver.get(url)

        # Wait for iframe and switch into it
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "content-iframe")))
        driver.switch_to.frame(driver.find_element(By.ID, "content-iframe"))
        time.sleep(1)

        try:
            # Remove any old CSVs
            for f in glob.glob(os.path.join(DOWNLOAD_DIR, "*.csv")):
                os.remove(f)

            # Find and click the button
            button = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//button[text()='Download CSV']"))
            )
            driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", button)
            time.sleep(1)
            WebDriverWait(driver, 5).until(EC.element_to_be_clickable(button)).click()
            print("✅ Download initiated")

            # Wait for file to download
            for _ in range(20):
                files = glob.glob(os.path.join(DOWNLOAD_DIR, "*.csv"))
                if files:
                    time.sleep(1)
                    break
                time.sleep(0.5)
            else:
                print("❌ CSV download timed out")
                driver.switch_to.default_content()
                continue

            # Extract URL parameters
            location_value = url.split("location=")[1].split("&")[0]
            status_value = url.split("status=")[1].split("&")[0]
            event_value = url.split("event=")[1].split("&")[0] if "event=" in url else ""

            # Read CSV and keep only first 7 columns
            csv_path = files[0]
            df = pd.read_csv(csv_path, header=0)
            df = df.iloc[:, :7]
            df.columns = ["Division", "Weight Class", "Lift", "Name", "Kilos", "Pounds", "Date"]

            # Add metadata columns
            df["Location"] = location_value
            df["Event"] = event_value
            df["Status"] = status_value

            all_data.append(df)
            print(f"📥 Loaded: {os.path.basename(csv_path)}")

        except Exception as e:
            print(f"⚠️ No download button found or failed: {e}")
            driver.save_screenshot(f"debug_{url.split('=')[-1]}.png")

        driver.switch_to.default_content()

    except Exception as e:
        print(f"❌ Error loading {url}: {e}")
        continue

driver.quit()

# === Save combined clean CSV ===
if all_data:
    combined_df = pd.concat(all_data, ignore_index=True)
    output_file = "uspa_all_records.csv"
    combined_df.to_csv(output_file, index=False)
    print(f"\n✅ All records saved to: {output_file}")
else:
    print("\n⚠️ No CSVs were collected.")

# === Cleanup
shutil.rmtree(DOWNLOAD_DIR, ignore_errors=True)
