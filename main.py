import os
import json
import gspread
import pandas as pd
import requests
import io
import time
from datetime import datetime, timedelta
import pytz
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe

# ==========================================
# 1. KONFIGURASI KEAMANAN & KREDENSIAL
# ==========================================
bearer_token = os.environ['BEARER_TOKEN']
service_account_info = json.loads(os.environ['GOOGLE_SERVICE_ACCOUNT'])

with open('credentials.json', 'w') as f:
    json.dump(service_account_info, f)

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
gc = gspread.authorize(creds)

SPREADSHEET_ID = '1XxA_LgepJhqjoPFx5bZNIp0J57ss_uhqjNk6uxs9cYU'
WAREHOUSE_ID = "7cc6cf47-973c-485c-b605-0e2ae40ec1d6"
TENANT_ID = "7b25e5bd-f47c-42ae-bf10-660e9fdbf78f"
REQUEST_ID = "8d3ec4e2-50bb-47c4-a257-f1cc5e635ad1"

HEADERS = {
    "accept": "application/json",
    "authorization": f"Bearer {bearer_token}",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

# ==========================================
# 2. LOGIKA ROLLING WINDOW (1 Bulan Lalu - Besok)
# ==========================================
today = datetime.now()
end_date = (today + timedelta(days=1)).strftime('%Y-%m-%d')

first_day_this_month = today.replace(day=1)
first_day_last_month = (first_day_this_month - timedelta(days=1)).replace(day=1)
start_date = first_day_last_month.strftime('%Y-%m-%d')

print(f"🔄 Menjalankan Rolling Window: {start_date} s/d {end_date}\n")

# Header baku untuk MTD_Fulfillment. Hanya kolom dengan nama yang cocok
# persis dengan daftar ini yang akan disalin dari sumber ke Google Sheets.
FULFILLMENT_HEADER = [
    "Nama Penyewa", "SKU Produk", "Nama Produk", "Type Produk", "Jumlah Pesanan",
    "UoM", "Jumlah IKU", "Jumlah BKU", "Harga Satuan", "Subtotal",
    "Tempat Penyimpanan", "Gudang Fisik", "Kode Fulfillment", "Invoice Platform",
    "Platform", "Nama Pemesan", "Alamat Pemesan", "Kecamatan Pemesan",
    "Kabupaten/Kota Pemesan", "No. Telp Pemesan", "Nama Penerima", "Alamat Penerima",
    "Kecamatan Penerima", "Kabupaten/Kota Penerima", "Latitude Penerima",
    "Longitude Penerima", "No. Telp Penerima", "Catatan", "Estimasi Berat (Kg)",
    "Estimasi Volume (cm3)", "Jumlah Koli", "Metode Pengantaran", "Kurir Pengantaran",
    "Tipe Pengantaran", "Nomor AWB", "Nama Kurir", "Tanggal Perkiraan Pickup",
    "Jam Perkiraan Pickup", "Status Fulfillment", "Alasan Fulfillment",
    "Tingkat Layanan Fulfillment", "Tanggal Fulfillment Masuk FF",
    "Jam Fulfillment Masuk FF", "Tanggal Fulfillment Allocated",
    "Jam Fulfillment Allocated", "Tanggal Batas Waktu", "Jam Batas Waktu",
    "Tanggal Proses Fulfillment", "Jam Proses Fulfillment", "Nama LM yang Proses",
    "Tanggal Fulfillment Menunggu Diambil", "Jam Fulfillment Menunggu Diambil",
    "Nama LM Menunggu Diambil", "Tanggal Fulfillment di Handover",
    "Jam Fulfillment di Handover", "Nama LM yang Handover", "Tanggal Live Resi",
    "Jam Live Resi", "Tanggal Ekspedisi Terkirim", "Jam Ekspedisi Terkirim",
    "Tanggal P-POD Diterima", "Jam P-POD Diterima", "Nama LM yang Menerima P-POD",
    "Tanggal Fulfillment Permintaan Dibatalkan", "Jam Fulfillment Permintaan Dibatalkan",
    "Nama Requestor Permintaan Dibatalkan", "Tanggal Fulfillment Dibatalkan",
    "Jam Fulfillment Dibatalkan", "Nama LM Proses Pembatalan Fulfillment",
    "Status Omni Channel"
]

# Header baku untuk MTD_Returns. Hanya kolom dengan nama yang cocok
# persis dengan daftar ini yang akan disalin dari sumber ke Google Sheets.
RETURNS_HEADER = [
    "Kode Pengembalian", "Kode Fulfillment", "Booking ID Pengembalian", "Nama Penyewa",
    "Tempat Penyimpanan", "Status Pengembalian", "Kode SKU", "Nama Produk",
    "Kode IKU", "Kode BKU", "Loker/Bin", "Pallet Loker/Bin", "Status IKU/BKU",
    "Pengembalian Didaftar", "Didaftarkan Oleh", "Pengembalian Diminta",
    "Pengembalian Diterima", "Diterima Oleh", "Pengembalian Diproses", "Diproses Oleh",
    "Pengembalian Selesai", "Diselesaikan Oleh", "Penyatuan", "Disatukan Oleh",
    "Dibatalkan", "Tipe Pengembalian", "Alasan Pengembalian", "Catatan Penerima",
    "Kurir Pengantaran Fulfillment", "Tipe Pengantaran Fulfillment"
]

# ==========================================
# 3. FUNGSI BANTUAN (HELPER)
# ==========================================
def get_fresh_export_token():
    url = "https://api.freshfactory.id/v1/admin/exports/generate_token"
    for _ in range(3):
        try:
            response = requests.get(url, headers=HEADERS)
            if response.status_code == 200:
                return response.json().get('data', {}).get('exportToken')
        except Exception:
            pass
        time.sleep(2)
    return None

def update_cache_log(category_name, status_message):
    try:
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        try:
            cache_sheet = spreadsheet.worksheet("Cache_Data")
        except gspread.exceptions.WorksheetNotFound:
            print("Membuat tab Cache_Data baru...")
            cache_sheet = spreadsheet.add_worksheet(title="Cache_Data", rows="100", cols="3")
            cache_sheet.append_row(["Kategori", "Terakhir Update (WIB)", "Status Eksekusi Terakhir"])

        # Konversi waktu ke WIB (Waktu Indonesia Barat)
        wib_tz = pytz.timezone('Asia/Jakarta')
        current_time_wib = datetime.now(wib_tz).strftime('%Y-%m-%d %H:%M:%S')

        # Cari baris yang sesuai dengan kategori
        existing_data = cache_sheet.get_all_values()
        row_index = -1
        for i, row in enumerate(existing_data):
            if row and row[0] == category_name:
                row_index = i + 1
                break

        if row_index != -1:
            # Update baris yang ada
            cache_sheet.update(f'B{row_index}:C{row_index}', [[current_time_wib, status_message]])
        else:
            # Tambahkan baris baru jika belum ada
            cache_sheet.append_row([category_name, current_time_wib, status_message])

    except Exception as e:
         print(f"⚠️ Peringatan: Gagal memperbarui Cache_Data: {e}")


def sanitize_dataframe_for_sheets(df):
    """
    Bersihkan DataFrame agar aman ditulis ke Google Sheets API:
    - NaN/NaT -> string kosong ('')
    - inf/-inf -> string kosong ('')
    - Timestamp/datetime -> string 'YYYY-MM-DD' atau 'YYYY-MM-DD HH:MM:SS'
    - Semua kolom lain dipaksa jadi string agar tidak ada objek non-JSON-serializable
      (Timestamp mentah dari pandas adalah penyebab umum error [500] saat set_with_dataframe)
    """
    df = df.copy()

    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].apply(
                lambda x: '' if pd.isna(x) else (
                    x.strftime('%Y-%m-%d') if x.time() == datetime.min.time()
                    else x.strftime('%Y-%m-%d %H:%M:%S')
                )
            )
        else:
            # Ganti inf/-inf jadi NaN dulu supaya tertangkap oleh fillna di bawah
            df[col] = df[col].replace([float('inf'), float('-inf')], pd.NA)

    # Ganti semua NaN/NaT/None yang tersisa jadi string kosong
    df = df.fillna('')

    # Paksa semua jadi string biar aman untuk JSON payload Sheets API
    df = df.astype(str)
    # astype(str) kadang menghasilkan literal 'nan'/'NaT'/'None' pada sel tertentu -> bersihkan
    df = df.replace(['nan', 'NaT', 'None', '<NA>'], '')

    return df


def write_with_retry(worksheet, df_to_write, max_retries=4, base_delay=3):
    """
    Tulis DataFrame ke worksheet dengan retry + exponential backoff.
    Menangani error transient Google Sheets API seperti [500] Internal error,
    [502]/[503] Service Unavailable, dan [429] Rate Limit Exceeded.
    """
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            worksheet.clear()
            set_with_dataframe(worksheet, df_to_write)
            return True
        except gspread.exceptions.APIError as e:
            last_error = e
            status_code = None
            try:
                status_code = e.response.status_code
            except Exception:
                pass

            if status_code in (500, 502, 503, 429):
                delay = base_delay * (2 ** (attempt - 1))
                print(f"⚠️ Percobaan {attempt}/{max_retries} gagal (HTTP {status_code}). Coba lagi dalam {delay}s...")
                time.sleep(delay)
                continue
            else:
                # Error non-transient (mis. permission, invalid range) -> jangan retry, lempar langsung
                raise

    # Semua percobaan retry habis, lempar error terakhir
    raise last_error


def fetch_and_replace(export_url, params, tab_name, fixed_header=None):
    print(f"--- Memproses {tab_name} ---")

    response = requests.get(export_url, headers=HEADERS, params=params)
    if response.status_code != 200:
        print(f"❌ Error {response.status_code}: Gagal menarik data.")
        # STANDAR LOG 1: FAILED (API)
        update_cache_log(tab_name, f"Failed (API Error {response.status_code})")
        return

    df_fresh = pd.read_excel(io.BytesIO(response.content))
    if df_fresh.empty:
        print("⚠️ Tidak ada data baru untuk periode ini.\n")
        # STANDAR LOG 2: SUCCESSFUL (KOSONG) - Tetap dicatat di Cache_Data
        update_cache_log(tab_name, "Successful (No New Data)")
        return

    try:
        worksheet = gc.open_by_key(SPREADSHEET_ID).worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        print(f"❌ Tab '{tab_name}' tidak ditemukan di Google Sheets.\n")
        # STANDAR LOG 3: FAILED (TAB HILANG)
        update_cache_log(tab_name, "Failed (Schema Error - Tab Missing)")
        return

    if fixed_header is not None:
        # Header baku ditentukan secara eksplisit (mis. MTD_Fulfillment), tidak
        # bergantung pada header yang saat ini ada di sheet.
        existing_header = fixed_header
    else:
        # Ambil header yang sudah ada di baris pertama sheet, agar header selalu dipertahankan
        existing_header = [h for h in worksheet.row_values(1) if h != ""]

    if existing_header:
        # Cocokkan kolom sumber (API) dengan header sheet berdasarkan NAMA, fleksibel terhadap
        # perubahan urutan/jumlah kolom di sumber:
        #  - Header yang namanya cocok dengan sumber -> diisi datanya
        #  - Header yang tidak ada di sumber -> tetap ada di sheet, tapi datanya kosong
        #  - Kolom sumber yang tidak ada di header -> diabaikan (tidak ditulis)
        matched_cols = [c for c in existing_header if c in df_fresh.columns]
        unmatched_cols = [c for c in existing_header if c not in df_fresh.columns]
        if unmatched_cols:
            print(f"ℹ️ Header tanpa kecocokan di sumber (dikosongkan): {unmatched_cols}")
        print(f"Kolom tercocokkan: {len(matched_cols)}/{len(existing_header)}")

        df_to_write = df_fresh.reindex(columns=existing_header)
    else:
        # Sheet/tab masih kosong -> pakai header dari sumber sebagai inisialisasi awal
        print("Header sheet kosong, menggunakan header dari sumber sebagai inisialisasi.")
        df_to_write = df_fresh

    # Bersihkan NaN/NaT/inf/Timestamp mentah sebelum dikirim ke Sheets API.
    # Ini mencegah error [500] Internal error yang sering dipicu oleh nilai
    # non-JSON-serializable (terutama pada kolom tanggal/jam yang banyak di MTD_Fulfillment).
    df_to_write = sanitize_dataframe_for_sheets(df_to_write)

    # Clear & paste, tapi header selalu dipertahankan/di-set ulang sesuai df_to_write.columns.
    # Ditulis dengan retry supaya error transient Google Sheets API (500/502/503/429)
    # tidak langsung menggagalkan seluruh pipeline.
    try:
        write_with_retry(worksheet, df_to_write)
    except gspread.exceptions.APIError as e:
        print(f"❌ Gagal menulis ke '{tab_name}' setelah beberapa percobaan: {e}\n")
        update_cache_log(tab_name, "Failed (Sheets API Error After Retries)")
        return

    print(f"✅ Berhasil Diperbarui! Header dipertahankan, {len(df_to_write)} baris data ditulis.\n")

    # STANDAR LOG 5: SUCCESSFUL (Berhasil diproses)
    update_cache_log(tab_name, "Successful")


# ==========================================
# 4. EKSEKUSI PIPELINE (TOKEN-PER-REQUEST)
# ==========================================
export_url = "https://api.freshfactory.id/v1/admin/exports"

# --- FULFILLMENT ---
f_token = get_fresh_export_token()
if f_token:
    fetch_and_replace(export_url, {
        "type": "fulfillment_detail", "warehouse_id": WAREHOUSE_ID, "tenant_id": TENANT_ID,
        "start_order_date_in": start_date, "end_order_date_in": end_date,
        "page": "1", "limit": "15", "token": f_token, "id": REQUEST_ID
    }, "MTD_Fulfillment", fixed_header=FULFILLMENT_HEADER)
else:
    error_msg = "❌ ERROR: Gagal mendapatkan token Fulfillment. Bearer Token kemungkinan kedaluwarsa!"
    print(error_msg)
    update_cache_log("MTD_Fulfillment", "Failed (Token Expired)")

# --- DELIVERY ---
d_token = get_fresh_export_token()
if d_token:
    fetch_and_replace(export_url, {
        "type": "delivery", "warehouseId": WAREHOUSE_ID, "tenantId": TENANT_ID,
        "startDate": start_date, "endDate": end_date, "updateStatus": "false",
        "token": d_token, "id": REQUEST_ID
    }, "MTD_Delivery")
else:
    error_msg = "❌ ERROR: Gagal mendapatkan token Delivery. Bearer Token kemungkinan kedaluwarsa!"
    print(error_msg)
    update_cache_log("MTD_Delivery", "Failed (Token Expired)")

# --- RETURNS ---
r_token = get_fresh_export_token()
if r_token:
    fetch_and_replace(export_url, {
        "type": "return", "warehouseId": WAREHOUSE_ID, "tenantId": TENANT_ID,
        "startDate": start_date, "endDate": end_date,
        "token": r_token, "id": REQUEST_ID
    }, "MTD_Returns", fixed_header=RETURNS_HEADER)
else:
    error_msg = "❌ ERROR: Gagal mendapatkan token Returns. Bearer Token kemungkinan kedaluwarsa!"
    print(error_msg)
    update_cache_log("MTD_Returns", "Failed (Token Expired)")

print("🎉 SINKRONISASI TUNTAS 100%.")
