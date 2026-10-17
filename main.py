import os
import sys
import json
import gspread
import pandas as pd
import requests
import io
import time
import traceback
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe

REQUEST_TIMEOUT = 30       # detik, untuk request ringan (token generation)
EXPORT_TIMEOUT = 300       # detik, untuk download export biasa (5 menit)
EXPORT_MAX_RETRIES = 3     # retry hanya untuk kegagalan NYATA (connection error / 5xx),
                           # BUKAN untuk timeout murni -- lihat catatan di fetch_export_data()

# ==========================================
# 1. KONFIGURASI KEAMANAN & KREDENSIAL
# ==========================================
def load_config():
    """
    Validasi seluruh environment variables & inisialisasi client di awal
    tanpa menulis file kredensial sementara ke disk (in-memory auth).
    """
    required_env_vars = [
        "BEARER_TOKEN",
        "GOOGLE_SERVICE_ACCOUNT",
        "SPREADSHEET_ID",
        "WAREHOUSE_ID",
        "TENANT_ID",
        "REQUEST_ID",
        "API_EXPORT_URL",
        "API_TOKEN_URL",
    ]

    config = {}
    missing_vars = []

    for var in required_env_vars:
        val = os.environ.get(var, "").strip()
        if not val:
            missing_vars.append(var)
        else:
            config[var] = val

    if missing_vars:
        print(f"❌ FATAL: Environment variable berikut tidak ditemukan / kosong: {', '.join(missing_vars)}")
        sys.exit(1)

    try:
        service_account_info = json.loads(config["GOOGLE_SERVICE_ACCOUNT"])
    except json.JSONDecodeError as e:
        print(f"❌ FATAL: 'GOOGLE_SERVICE_ACCOUNT' bukan JSON yang valid: {e}")
        sys.exit(1)

    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        # Gunakan in-memory dictionary agar kredensial tidak pernah ditulis ke disk
        creds = ServiceAccountCredentials.from_json_keyfile_dict(service_account_info, scope)
        gc = gspread.authorize(creds)
    except Exception as e:
        print(f"❌ FATAL: Gagal autentikasi ke Google Sheets. Periksa isi GOOGLE_SERVICE_ACCOUNT.\nDetail: {e}")
        sys.exit(1)

    return config, gc


CONFIG, gc = load_config()

SPREADSHEET_ID = CONFIG["SPREADSHEET_ID"]
WAREHOUSE_ID = CONFIG["WAREHOUSE_ID"]
TENANT_ID = CONFIG["TENANT_ID"]
REQUEST_ID = CONFIG["REQUEST_ID"]
EXPORT_URL = CONFIG["API_EXPORT_URL"]
TOKEN_URL = CONFIG["API_TOKEN_URL"]

HEADERS = {
    "accept": "application/json",
    "authorization": f"Bearer {CONFIG['BEARER_TOKEN']}",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
}

# Buka spreadsheet SEKALI dan reuse handle-nya di seluruh script.
try:
    SPREADSHEET = gc.open_by_key(SPREADSHEET_ID)
except gspread.exceptions.SpreadsheetNotFound:
    print("❌ FATAL: Target Spreadsheet tidak ditemukan atau service account tidak punya akses.")
    sys.exit(1)
except gspread.exceptions.APIError as e:
    print(f"❌ FATAL: Gagal membuka spreadsheet (Google Sheets API error): {e}")
    sys.exit(1)
except Exception as e:
    print(f"❌ FATAL: Gagal membuka spreadsheet karena error tak terduga: {e}")
    sys.exit(1)

HTTP_SESSION = requests.Session()
HTTP_SESSION.headers.update(HEADERS)

# ==========================================
# 2. LOGIKA ROLLING WINDOW (1 Bulan Lalu - Besok)
# ==========================================
today = datetime.now()
end_date = (today + timedelta(days=1)).strftime('%Y-%m-%d')

first_day_this_month = today.replace(day=1)
first_day_last_month = (first_day_this_month - timedelta(days=1)).replace(day=1)
start_date = first_day_last_month.strftime('%Y-%m-%d')

print(f"🔄 Menjalankan Rolling Window: {start_date} s/d {end_date}\n")

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
    "Tingkat Layanan Fulfillment", "Tanggal Fulfillment Masuk WMS",
    "Jam Fulfillment Masuk WMS", "Tanggal Fulfillment Allocated",
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

JOBS = [
    {
        "tab_name": "MTD_Fulfillment",
        "fixed_header": FULFILLMENT_HEADER,
        "params": {
            "type": "fulfillment_detail", "warehouse_id": WAREHOUSE_ID, "tenant_id": TENANT_ID,
            "start_order_date_in": start_date, "end_order_date_in": end_date,
            "page": "1", "limit": "15",
        },
        "timeout": 600,
    },
    {
        "tab_name": "MTD_Delivery",
        "fixed_header": None,
        "params": {
            "type": "delivery", "warehouseId": WAREHOUSE_ID, "tenantId": TENANT_ID,
            "startDate": start_date, "endDate": end_date, "updateStatus": "false",
        },
        "timeout": EXPORT_TIMEOUT,
    },
    {
        "tab_name": "MTD_Returns",
        "fixed_header": RETURNS_HEADER,
        "params": {
            "type": "return", "warehouseId": WAREHOUSE_ID, "tenantId": TENANT_ID,
            "startDate": start_date, "endDate": end_date,
        },
        "timeout": EXPORT_TIMEOUT,
    },
]

# ==========================================
# 3. FUNGSI BANTUAN (HELPER)
# ==========================================
def get_export_token():
    """
    Ambil token export dari endpoint API_TOKEN_URL.
    """
    last_error = None

    for attempt in range(1, 4):
        try:
            response = HTTP_SESSION.get(TOKEN_URL, timeout=REQUEST_TIMEOUT)
        except requests.exceptions.Timeout:
            last_error = "timeout"
        except requests.exceptions.RequestException as e:
            last_error = f"connection error ({e})"
        else:
            if response.status_code == 200:
                try:
                    token = response.json().get('data', {}).get('exportToken')
                except (ValueError, AttributeError):
                    token = None
                if token:
                    return token
                last_error = "response 200 tapi tidak berisi exportToken (kemungkinan payload tidak terduga)"
            else:
                last_error = f"HTTP {response.status_code}"

        if attempt < 3:
            time.sleep(2)

    print(f"⚠️ Gagal mendapatkan export token setelah 3 percobaan. Error terakhir: {last_error}")
    return None


def update_cache_log(category_name, status_message):
    """
    Catat status eksekusi ke tab Cache_Data.
    """
    try:
        try:
            cache_sheet = SPREADSHEET.worksheet("Cache_Data")
        except gspread.exceptions.WorksheetNotFound:
            print("Membuat tab Cache_Data baru...")
            cache_sheet = SPREADSHEET.add_worksheet(title="Cache_Data", rows="100", cols="3")
            cache_sheet.append_row(["Kategori", "Terakhir Update (WIB)", "Status Eksekusi Terakhir"])

        wib_tz = pytz.timezone('Asia/Jakarta')
        current_time_wib = datetime.now(wib_tz).strftime('%Y-%m-%d %H:%M:%S')

        try:
            cell = cache_sheet.find(category_name, in_column=1)
        except gspread.exceptions.CellNotFound:
            cell = None

        if cell is not None:
            cache_sheet.update(f'B{cell.row}:C{cell.row}', [[current_time_wib, status_message]])
        else:
            cache_sheet.append_row([category_name, current_time_wib, status_message])

    except Exception:
        print(f"⚠️ Peringatan: Gagal memperbarui Cache_Data untuk '{category_name}':")
        print(traceback.format_exc())


def sanitize_dataframe_for_sheets(df):
    """
    Bersihkan DataFrame agar aman ditulis ke Google Sheets API.
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
            df[col] = df[col].replace([float('inf'), float('-inf')], pd.NA)

    df = df.fillna('')
    df = df.astype(str)
    df = df.replace(['nan', 'NaT', 'None', '<NA>'], '')

    return df


def write_with_retry(worksheet, df_to_write, max_retries=4, base_delay=3):
    """
    Tulis DataFrame ke worksheet dengan retry + exponential backoff untuk
    error transient Google Sheets API: [500], [502], [503], [429].
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
                raise

    raise last_error


def fetch_export_data(job):
    """
    Tahap fetch (I/O-bound): token + download + parse ke DataFrame.
    Dijalankan paralel untuk ke-3 tipe data.
    """
    tab_name = job["tab_name"]
    print(f"--- Mengambil data {tab_name} ---")

    timeout = job.get("timeout", EXPORT_TIMEOUT)

    try:
        last_status = None

        for attempt in range(1, EXPORT_MAX_RETRIES + 1):
            token = get_export_token()
            if not token:
                print(f"❌ ERROR: Gagal mendapatkan token untuk {tab_name}. Bearer Token kemungkinan kedaluwarsa!")
                return tab_name, None, "Failed (Token Expired)"

            params = dict(job["params"])
            params["token"] = token
            params["id"] = REQUEST_ID

            try:
                response = HTTP_SESSION.get(EXPORT_URL, params=params, timeout=timeout)
            except requests.exceptions.Timeout:
                print(f"❌ Timeout saat mengambil {tab_name} setelah menunggu {timeout}s. "
                      f"Tidak diulang otomatis (retry akan me-restart generation, bukan melanjutkan).")
                return tab_name, None, "Failed (Request Timeout)"
            except requests.exceptions.RequestException as e:
                last_status = "Failed (Connection Error)"
                print(f"⚠️ Error koneksi saat mengambil {tab_name} (percobaan {attempt}/{EXPORT_MAX_RETRIES}): {e}")
            else:
                if response.status_code != 200:
                    last_status = f"Failed (API Error {response.status_code})"
                    if response.status_code < 500:
                        print(f"❌ Error {response.status_code}: Gagal menarik data {tab_name} (tidak akan diulang).")
                        return tab_name, None, last_status
                    print(f"⚠️ Error {response.status_code} saat mengambil {tab_name} (percobaan {attempt}/{EXPORT_MAX_RETRIES}).")
                else:
                    try:
                        df_raw = pd.read_excel(io.BytesIO(response.content))
                    except Exception as e:
                        print(f"❌ Gagal parse file Excel untuk {tab_name}: {e}")
                        return tab_name, None, "Failed (Invalid Excel Response)"

                    if df_raw.empty:
                        print(f"⚠️ Tidak ada data baru untuk {tab_name} pada periode ini.\n")
                        return tab_name, None, "Successful (No New Data)"

                    return tab_name, df_raw, None

            if attempt < EXPORT_MAX_RETRIES:
                delay = 5 * attempt
                print(f"   Coba lagi dalam {delay}s...")
                time.sleep(delay)

        print(f"❌ Gagal mengambil {tab_name} setelah {EXPORT_MAX_RETRIES} percobaan.")
        return tab_name, None, last_status or "Failed (Unknown Error)"

    except Exception:
        print(f"❌ Error tak terduga saat memproses {tab_name}:")
        print(traceback.format_exc())
        return tab_name, None, "Failed (Unexpected Error)"


def write_to_sheet(tab_name, df_raw, fixed_header):
    """
    Tahap write (Sheets API): cocokkan header, sanitize, tulis dengan retry.
    """
    try:
        try:
            worksheet = SPREADSHEET.worksheet(tab_name)
        except gspread.exceptions.WorksheetNotFound:
            print(f"❌ Tab '{tab_name}' tidak ditemukan di Google Sheets.\n")
            update_cache_log(tab_name, "Failed (Schema Error - Tab Missing)")
            return False

        if fixed_header is not None:
            existing_header = fixed_header
        else:
            existing_header = [h for h in worksheet.row_values(1) if h != ""]

        if existing_header:
            matched_cols = [c for c in existing_header if c in df_raw.columns]
            unmatched_cols = [c for c in existing_header if c not in df_raw.columns]
            if unmatched_cols:
                print(f"ℹ️ Header tanpa kecocokan di sumber (dikosongkan): {unmatched_cols}")
            print(f"Kolom tercocokkan: {len(matched_cols)}/{len(existing_header)}")

            df_to_write = df_raw.reindex(columns=existing_header)
        else:
            print("Header sheet kosong, menggunakan header dari sumber sebagai inisialisasi.")
            df_to_write = df_raw

        df_to_write = sanitize_dataframe_for_sheets(df_to_write)

        try:
            write_with_retry(worksheet, df_to_write)
        except gspread.exceptions.APIError as e:
            print(f"❌ Gagal menulis ke '{tab_name}' setelah beberapa percobaan: {e}\n")
            update_cache_log(tab_name, "Failed (Sheets API Error After Retries)")
            return False

        print(f"✅ Berhasil Diperbarui! Header dipertahankan, {len(df_to_write)} baris data ditulis.\n")
        update_cache_log(tab_name, "Successful")
        return True

    except Exception:
        print(f"❌ Error tak terduga saat menulis '{tab_name}':")
        print(traceback.format_exc())
        update_cache_log(tab_name, "Failed (Unexpected Error)")
        return False


# ==========================================
# 4. EKSEKUSI PIPELINE
# ==========================================
def main():
    job_status = {}

    fetch_results = {}
    with ThreadPoolExecutor(max_workers=len(JOBS)) as executor:
        futures = {executor.submit(fetch_export_data, job): job for job in JOBS}
        for future in as_completed(futures):
            job = futures[future]
            try:
                tab_name, df, status = future.result()
            except Exception:
                tab_name = job["tab_name"]
                print(f"❌ Error tak terduga dari thread fetch '{tab_name}':")
                print(traceback.format_exc())
                df, status = None, "Failed (Unexpected Thread Error)"
            fetch_results[tab_name] = (df, status)

    for job in JOBS:
        tab_name = job["tab_name"]
        df, status = fetch_results[tab_name]

        if status is not None:
            update_cache_log(tab_name, status)
            job_status[tab_name] = status.startswith("Successful")
            continue

        job_status[tab_name] = write_to_sheet(tab_name, df, job["fixed_header"])

    # ==========================================
    # 5. RINGKASAN & EXIT CODE
    # ==========================================
    print("=" * 50)
    print("📋 RINGKASAN EKSEKUSI:")
    any_failed = False
    for tab_name, ok in job_status.items():
        icon = "✅" if ok else "❌"
        print(f"  {icon} {tab_name}: {'Berhasil' if ok else 'Gagal'}")
        if not ok:
            any_failed = True
    print("=" * 50)

    if any_failed:
        print("⚠️ SINKRONISASI SELESAI DENGAN KEGAGALAN PADA SATU ATAU LEBIH PIPELINE.")
        sys.exit(1)
    else:
        print("🎉 SINKRONISASI TUNTAS 100%.")
        sys.exit(0)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception:
        print("❌ FATAL: Error tak terduga menghentikan seluruh pipeline:")
        print(traceback.format_exc())
        sys.exit(1)
