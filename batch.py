import csv
import string
import re
import os
from playwright.sync_api import sync_playwright

def scrape_basabali():
    # 1. Setup Folder Output
    output_folder = "result"
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"Folder '{output_folder}' telah dibuat.")

    # URL Hardcoded
    base_url = "https://dictionary.basabali.org/w/index.php?title=Special:RunQuery&form=DictionaryIndex&DictionaryIndex%5BLetter%5D=A&_run=1&redesign=yes"
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        for char in string.ascii_uppercase:
            current_url = re.sub(r'(Letter(?:%5D|\])=)[^&]*', rf'\1{char}', base_url)
            
            # 2. Tentukan path file di dalam folder result
            filename = os.path.join(output_folder, f"Letter {char}.tsv")
            
            print(f"--- Memproses Huruf {char} ---")

            try:
                page.goto(current_url, wait_until="domcontentloaded", timeout=60000)
                
                # XPath Container
                container_xpath = "/html/body/div[5]/div[2]/div[1]/div[1]/div[3]/div/div/div[2]"
                container_found = page.locator(f"xpath={container_xpath}")
                
                try:
                    container_found.wait_for(state="visible", timeout=7000)
                except:
                    print(f"Data untuk huruf {char} tidak ditemukan (skip).")
                    continue

                # Ambil hanya tag 'a' di dalam container
                elements = container_found.locator("a")
                count = elements.count()
                
                if count == 0:
                    print(f"Tidak ada data ditemukan untuk huruf {char}.")
                    continue

                # 3. Mode 'w' akan overwrite file yang sudah ada
                with open(filename, mode='w', newline='', encoding='utf-8') as file:
                    writer = csv.writer(file, delimiter='\t')
                    writer.writerow(["title", "text", "path"])
                    
                    for i in range(count):
                        el = elements.nth(i)
                        title = (el.get_attribute("title") or "").strip()
                        text = (el.inner_text() or "").strip()
                        href = (el.get_attribute("href") or "").strip()
                        
                        if href.startswith('/'):
                            href = f"https://dictionary.basabali.org{href}"
                        
                        writer.writerow([title, text, href])
                
                print(f"Berhasil! {count} data disimpan/di-overwrite ke {filename}")

            except Exception as e:
                print(f"Error pada huruf {char}: {e}")
                continue

        browser.close()
        print(f"\nProses selesai. Cek folder '{output_folder}' untuk hasilnya.")

if __name__ == "__main__":
    scrape_basabali()