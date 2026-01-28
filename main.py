import csv
import time
import re
from playwright.sync_api import sync_playwright

def scrape_to_tsv():
    url = input("Masukkan URL: ")
    
    # Ekstrak abjad dari URL untuk nama file
    match = re.search(r'Letter%5D=([^&]+)', url)
    if match:
        abjad = match.group(1)
        filename = f"Letter {abjad}.tsv"
    else:
        filename = "hasil_otomasi.tsv"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        )
        
        page = context.new_page()
        print(f"Memproses halaman untuk file: {filename}...")
        
        try:
            # Render web sampai 'masak'
            page.goto(url, wait_until="networkidle", timeout=60000)
            time.sleep(3) 

            # Ambil elemen berdasarkan XPath
            full_xpath = "/html/body/div[5]/div[2]/div[1]/div[1]/div[3]/div/div/div[2]"
            elements = page.locator(f"xpath={full_xpath}//a")
            
            count = elements.count()
            
            if count == 0:
                print("Data tidak ditemukan.")
            else:
                with open(filename, mode='w', newline='', encoding='utf-8') as file:
                    # Menggunakan tab sebagai pemisah
                    writer = csv.writer(file, delimiter='\t')
                    
                    # Header tanpa kolom 'No'
                    writer.writerow(["title", "text", "path"])
                    
                    for i in range(count):
                        title = (elements.nth(i).get_attribute("title") or "").strip()
                        text = (elements.nth(i).inner_text() or "").strip()
                        href = (elements.nth(i).get_attribute("href") or "").strip()
                        
                        # Tulis baris data
                        writer.writerow([title, text, href])
                        
                print(f"Berhasil! {count} data disimpan ke {filename}")
                    
        except Exception as e:
            print(f"Error: {e}")
        
        finally:
            browser.close()

if __name__ == "__main__":
    scrape_to_tsv()