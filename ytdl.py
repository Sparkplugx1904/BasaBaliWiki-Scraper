import pandas as pd
import yt_dlp
import os
import glob
import re
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# --- Konfigurasi ---
INPUT_DIR = './transcripts'
OUTPUT_DIR = './clips'
METADATA_FILE = 'metadata.tsv'
MAX_FILE_WORKERS = 10  # Memproses 10 file sekaligus

# Lock dan Tracking
file_lock = threading.Lock()
urls_lock = threading.Lock()
processed_urls = set()  # Menyimpan URL agar tidak diproses ulang dalam satu sesi

os.makedirs(OUTPUT_DIR, exist_ok=True)

def clean_filename(text):
    """Membersihkan nama file dari karakter terlarang."""
    return re.sub(r'[\\/*?:"<>|]', "", str(text))

def download_audio(row):
    """Fungsi inti untuk download audio dengan pengecekan duplikasi."""
    global processed_urls
    
    page_title = row.get('page_title', 'unknown')
    original_url = str(row.get('video_url', ''))
    
    if not original_url or "http" not in original_url:
        return None

    # 1. Cek duplikasi URL (hanya selama program berjalan)
    with urls_lock:
        if original_url in processed_urls:
            return None
        processed_urls.add(original_url)

    # FIX URL: Mengubah format embed ke format standar
    video_url = original_url.replace('embed/', 'watch?v=')

    # AMBIL ALFABET AWALAN
    first_char = str(page_title)[0].upper() if page_title else "X"
    
    # FORMAT: {Huruf}-{Timestamp}-{Judul}.wav
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    safe_title = clean_filename(page_title)
    filename_base = f"{first_char}-{timestamp}-{safe_title}"
    output_path = f"clips/{filename_base}.wav"
    
    ydl_opts = {
        'format': 'bestaudio/best',
        'outtmpl': os.path.join(OUTPUT_DIR, filename_base),
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'wav',
            'preferredquality': '192',
        }],
        'quiet': True,
        'no_warnings': True,
        'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])
        
        # Mengembalikan data TANPA page_title sesuai permintaan
        return {
            'path': output_path,
            'balinese': row.get('balinese', ''),
            'english': row.get('english', ''),
            'indonesian': row.get('indonesian', '')
        }
    except Exception as e:
        print(f" [!] Gagal download [{page_title}]: {e}")
        return None

def append_to_tsv(data, filename):
    """Menulis data ke TSV secara aman (thread-safe)."""
    with file_lock:
        file_exists = os.path.isfile(filename)
        df = pd.DataFrame([data])
        # Menulis ke file, page_title sudah tidak ada di dalam 'data'
        df.to_csv(filename, mode='a', sep='\t', index=False, header=not file_exists)

def process_single_letter(file_path):
    """Satu worker untuk satu file Transcript Letter."""
    letter_name = os.path.basename(file_path)
    print(f"[*] Memproses file: {letter_name}")
    
    try:
        df = pd.read_csv(file_path, sep='\t')
        rows = df.to_dict('records')
        
        count = 0
        for row in rows:
            result = download_audio(row)
            if result:
                append_to_tsv(result, METADATA_FILE)
                count += 1
        
        if count > 0:
            print(f"[V] SELESAI: {letter_name} | Berhasil: {count} item baru.")
    except Exception as e:
        print(f"[X] Error pada file {letter_name}: {e}")

def main():
    # Mencari file transkrip
    files = sorted(glob.glob(os.path.join(INPUT_DIR, "Transcript Letter *.tsv")))
    
    if not files:
        print(f"File tidak ditemukan di {INPUT_DIR}. Pastikan format namanya benar.")
        return

    print(f"--- Memulai Sesi ---")
    print(f"Maksimal Workers: {MAX_FILE_WORKERS}")
    print(f"Output Metadata: {METADATA_FILE}\n")

    with ThreadPoolExecutor(max_workers=MAX_FILE_WORKERS) as executor:
        executor.map(process_single_letter, files)

    print(f"\n--- Semua Proses Selesai ---")
    print(f"Total URL unik diproses: {len(processed_urls)}")
    print(f"Metadata tersimpan di: {METADATA_FILE}")

if __name__ == "__main__":
    main()