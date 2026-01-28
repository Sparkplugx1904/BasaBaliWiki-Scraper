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
    """Konfigurasi driver dengan proteksi memory leak."""
    chrome_options = Options()
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_argument("--window-size=1080,1000")
    
    # Argumen tambahan untuk efisiensi memori
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-application-cache")
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--no-first-run")
    chrome_options.add_argument("--disk-cache-size=1")
    chrome_options.add_argument("--media-cache-size=1")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    # Set timeout agar tidak menggantung selamanya
    driver.set_page_load_timeout(30)
    driver.set_script_timeout(30)
    
    return driver

def scroll_to_bottom(driver):
    """Fungsi scroll dengan pembatasan maksimal iterasi."""
    last_height = driver.execute_script("return document.body.scrollHeight")
    max_scrolls = 10  # Batasi agar tidak terjebak infinite scroll
    scroll_count = 0
    
    while scroll_count < max_scrolls:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1.5)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
        scroll_count += 1

def clean_text(text):
    if text:
        return " ".join(text.split())
    return ""

def process_file_worker(filename, input_folder, output_folder, fieldnames):
    """
    Worker ini menangani satu file TSV dengan mekanisme 
    RESTART BROWSER setiap 10 URL untuk mencegah memory leak.
    """
    input_path = os.path.join(input_folder, filename)
    output_path = os.path.join(output_folder, f"Transcript_{filename}")
    
    print(f"[+] Memulai pemrosesan file: {filename}")
    
    driver = setup_driver()
    total_videos_in_file = 0
    urls_processed_in_session = 0
    RESTART_THRESHOLD = 10 # Restart browser setiap 10 URL

    try:
        with open(input_path, mode='r', encoding='utf-8', errors='ignore') as f_in:
            reader = list(csv.DictReader(f_in, delimiter='\t'))
            
            for i, row in enumerate(reader):
                # Mekanisme Restart Browser
                if urls_processed_in_session >= RESTART_THRESHOLD:
                    print(f"[{filename}] Melakukan restart browser untuk membersihkan memori...")
                    driver.quit()
                    driver = setup_driver()
                    urls_processed_in_session = 0

                title = row.get('title', 'Unknown')
                url = row.get('path')
                if not url: continue

                print(f"[{filename}] Baris {i+1}/{len(reader)}: {title}")
                
                try:
                    driver.get(url)
                    urls_processed_in_session += 1
                    
                    # Tunggu konten dimuat
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "bali-item-group"))
                    )
                    
                    scroll_to_bottom(driver)
                    groups = driver.find_elements(By.CLASS_NAME, "bali-item-group")

                    extracted_data = []
                    for group in groups:
                        try:
                            # Cek iframe Youtube
                            iframes = group.find_elements(By.CSS_SELECTOR, "iframe[src*='youtube']")
                            if not iframes: continue

                            video_url = iframes[0].get_attribute("src")
                            if video_url.startswith("//"): video_url = "https:" + video_url

                            balinese_text = ""
                            english_text = ""
                            indo_text = ""

                            items = group.find_elements(By.CLASS_NAME, "bali-item")
                            for item in items:
                                try:
                                    label = item.find_element(By.CLASS_NAME, "bali-item__left").text.strip().lower()
                                    content = clean_text(item.find_element(By.CLASS_NAME, "bali-item__right").text)
                                    
                                    if "balinese" in label: balinese_text = content
                                    elif "english" in label: english_text = content
                                    elif "indonesian" in label: indo_text = content
                                except:
                                    continue

                            if video_url:
                                extracted_data.append({
                                    "page_title": title, "page_url": url, "video_url": video_url,
                                    "balinese": balinese_text, "english": english_text, "indonesian": indo_text
                                })
                        except:
                            continue

                    # Tulis hasil ke file secara batch per halaman
                    if extracted_data:
                        file_exists = os.path.isfile(output_path)
                        with open(output_path, mode='a', newline='', encoding='utf-8') as f:
                            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter='\t')
                            if not file_exists: writer.writeheader()
                            writer.writerows(extracted_data)
                        total_videos_in_file += len(extracted_data)

                except Exception as e:
                    print(f"Error pada URL {url}: {str(e)}")
                    continue
                    
    finally:
        if driver:
            driver.quit()
        print(f"✅ SELESAI: {filename} (Total: {total_videos_in_file} video)")

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    input_folder = os.path.join(base_dir, 'list')
    output_folder = os.path.join(base_dir, 'transcripts')
    fieldnames = ['page_title', 'page_url', 'video_url', 'balinese', 'english', 'indonesian']

    if not os.path.exists(input_folder):
        print("Folder 'list' tidak ditemukan!")
        return
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    files = sorted([f for f in os.listdir(input_folder) if f.endswith((".tsv", ".txt"))])

    print(f"🚀 Memulai scraping paralel ({len(files)} file, Max 3 thread).")
    # Disarankan max_workers kecil (misal 3) jika RAM terbatas karena Chrome memakan banyak RAM
    with ThreadPoolExecutor(max_workers=5) as executor:
        for filename in files:
            executor.submit(process_file_worker, filename, input_folder, output_folder, fieldnames)

    print("\n🏁 PROSES SELESAI!")

if __name__ == "__main__":
    main()