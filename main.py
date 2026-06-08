import os
import json
import gspread
import pandas as pd
import requests
import io
import time
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe, get_as_dataframe

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
REQUEST_ID = "402cdb02-16b3-4e54-83af-89d2a1ab2c16"

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

# ==========================================
# 3. FUNGSI BANTUAN (HELPER)
# ==========================================
def get_fresh_export_token():
    url = "https://api.freshfactory.id/v1/admin/exports/generate_token"
    for _ in range(3): # Coba 3x jika gagal
        try:
            response = requests.get(url, headers=HEADERS)
            if response.status_code == 200:
                return response.json().get('data', {}).get('exportToken')
        except Exception:
            pass
        time.sleep(2)
    return None

def fetch_upsert_and_push(export_url, params, tab_name, unique_keys):
    print(f"--- Memproses {tab_name} ---")
    
    # Menarik data menggunakan param dictionary agar URL aman
    response = requests.get(export_url, headers=HEADERS, params=params)
    if response.status_code != 200:
        print(f"❌ Error {response.status_code}: Gagal menarik data. Respons: {response.text}")
        return
        
    df_fresh = pd.read_excel(io.BytesIO(response.content))
    if df_fresh.empty:
        print("⚠️ Tidak ada data baru untuk periode ini.\n")
        return

    try:
        worksheet = gc.open_by_key(SPREADSHEET_ID).worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        print(f"❌ Tab '{tab_name}' tidak ditemukan di Google Sheets.\n")
        return

    # Menarik data dari Sheet menggunakan gspread_dataframe (lebih cepat & bersih)
    existing_df = get_as_dataframe(worksheet).dropna(how='all').dropna(axis=1, how='all')
    
    if existing_df.empty:
        final_df = df_fresh
        print(f"Inisialisasi awal, {len(final_df)} baris disiapkan.")
    else:
        # Pengecekan Composite Keys
        missing_cols = [col for col in unique_keys if col not in df_fresh.columns or col not in existing_df.columns]
        if missing_cols:
            print(f"❌ ERROR KRITIS: Kolom unik {missing_cols} tidak ditemukan di Sheet/API.")
            return

        # Simpan struktur header asli milik Google Sheet untuk merapikan urutan
        existing_headers = existing_df.columns.tolist()

        # Proses Penggabungan & Deduplikasi (Upsert)
        combined_df = pd.concat([existing_df, df_fresh], ignore_index=True)
        final_df = combined_df.drop_duplicates(subset=unique_keys, keep='last')
        
        # --- FITUR KESELAMATAN KOLOM ---
        # Memaksa data baru mengikuti urutan nama kolom yang sudah ada di Sheet
        final_df = final_df.reindex(columns=existing_headers)
        
        updated_rows = len(final_df) - len(existing_df)
        print(f"Upsert & Column Mapping selesai. Penambahan bersih: {updated_rows} baris.")

    # Membersihkan dan memasukkan DataFrame secara otomatis dengan gspread_dataframe
    worksheet.clear()
    set_with_dataframe(worksheet, final_df)
    print(f"✅ Data {tab_name} berhasil di-push sesuai format kolom!\n")

# ==========================================
# 4. EKSEKUSI PIPELINE (TOKEN-PER-REQUEST)
# ==========================================
export_url = "https://api.freshfactory.id/v1/admin/exports"

# --- FULFILLMENT ---
f_token = get_fresh_export_token()
if f_token:
    fetch_upsert_and_push(export_url, {
        "type": "fulfillment_detail", "warehouse_id": WAREHOUSE_ID, "tenant_id": TENANT_ID,
        "start_order_date_in": start_date, "end_order_date_in": end_date,
        "page": "1", "limit": "15", "token": f_token, "id": REQUEST_ID
    }, "MTD_Fulfillment", ["Kode Fulfillment", "SKU Produk"])

# --- DELIVERY ---
d_token = get_fresh_export_token()
if d_token:
    fetch_upsert_and_push(export_url, {
        "type": "delivery", "warehouseId": WAREHOUSE_ID, "tenantId": TENANT_ID,
        "startDate": start_date, "endDate": end_date, "updateStatus": "false",
        "token": d_token, "id": REQUEST_ID
    }, "MTD_Delivery", ["Kode Pengiriman", "SKU Produk"])

# --- RETURNS ---
r_token = get_fresh_export_token()
if r_token:
    fetch_upsert_and_push(export_url, {
        "type": "return", "warehouseId": WAREHOUSE_ID, "tenantId": TENANT_ID,
        "startDate": start_date, "endDate": end_date,
        "token": r_token, "id": REQUEST_ID
    }, "MTD_Returns", ["Kode Fulfillment", "Kode SKU"])

print("🎉 SINKRONISASI TUNTAS 100%.")
