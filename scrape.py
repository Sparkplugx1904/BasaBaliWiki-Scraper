import csv
import os
import time
from concurrent.futures import ThreadPoolExecutor
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

def setup_driver():
    chrome_options = Options()
    # Tanpa headless
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_argument("--window-size=1000,800")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    return driver

def scroll_to_bottom(driver):
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

def clean_text(text):
    if text:
        return text.replace('\t', ' ').replace('\n', ' ').strip()
    return ""

def process_file_worker(filename, input_folder, output_folder, fieldnames):
    """
    Worker ini menangani SATU file TSV secara utuh menggunakan SATU browser.
    """
    input_path = os.path.join(input_folder, filename)
    output_path = os.path.join(output_folder, f"Transcript {filename}")
    
    print(f"[+] Memulai thread untuk file: {filename}")
    
    driver = setup_driver()
    total_videos_in_file = 0

    try:
        with open(input_path, mode='r', encoding='utf-8', errors='ignore') as f_in:
            reader = csv.DictReader(f_in, delimiter='\t')
            
            for i, row in enumerate(reader):
                title = row.get('title', 'Unknown')
                url = row.get('path')
                if not url: continue

                # Log proses per file agar tidak pusing melihatnya
                print(f"[{filename}] Memproses baris {i+1}: {title}")
                
                # Proses scraping halaman
                try:
                    driver.get(url)
                    # Wait element
                    WebDriverWait(driver, 7).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "bali-item-group"))
                    )
                    
                    scroll_to_bottom(driver)
                    groups = driver.find_elements(By.CLASS_NAME, "bali-item-group")

                    for index, group in enumerate(groups):
                        iframes = group.find_elements(By.CSS_SELECTOR, "iframe[src*='youtube']")
                        if not iframes: continue

                        video_url = iframes[0].get_attribute("src")
                        if video_url.startswith("//"): video_url = "https:" + video_url

                        balinese_text = ""
                        english_text = ""
                        indo_text = ""

                        items = group.find_elements(By.CLASS_NAME, "bali-item")
                        for item in items:
                            label_elems = item.find_elements(By.CLASS_NAME, "bali-item__left")
                            content_elems = item.find_elements(By.CLASS_NAME, "bali-item__right")
                            if not label_elems or not content_elems: continue

                            label = label_elems[0].text.strip().lower()
                            content = clean_text(content_elems[0].text)

                            if "balinese" in label: balinese_text = content
                            elif "english" in label: english_text = content
                            elif "indonesian" in label: indo_text = content

                        if video_url:
                            data = {
                                "page_title": title, "page_url": url, "video_url": video_url,
                                "balinese": balinese_text, "english": english_text, "indonesian": indo_text
                            }
                            
                            file_exists = os.path.isfile(output_path)
                            with open(output_path, mode='a', newline='', encoding='utf-8') as f:
                                writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter='\t')
                                if not file_exists: writer.writeheader()
                                writer.writerow(data)
                            total_videos_in_file += 1
                except Exception:
                    continue
                    
    finally:
        driver.quit()
        print(f"✅ SELESAI: {filename} (Total: {total_videos_in_file} video)")

def main():
    # Folder Management
    base_dir = os.path.dirname(os.path.abspath(__file__))
    input_folder = os.path.join(base_dir, 'list')
    output_folder = os.path.join(base_dir, 'transcripts')
    fieldnames = ['page_title', 'page_url', 'video_url', 'balinese', 'english', 'indonesian']

    if not os.path.exists(input_folder):
        print("Folder 'list' tidak ada!")
        return
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # List semua file TSV/TXT
    files = sorted([f for f in os.listdir(input_folder) if f.endswith((".tsv", ".txt"))])

    print(f"🚀 Menjalankan Parallel Processing untuk {len(files)} file (Max 5 thread).")

    # Eksekusi paralel per file
    with ThreadPoolExecutor(max_workers=2) as executor:
        for filename in files:
            executor.submit(process_file_worker, filename, input_folder, output_folder, fieldnames)

    print("\n🏁 SEMUA FILE SELESAI DIPROSES!")

if __name__ == "__main__":
    main()