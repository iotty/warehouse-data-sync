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

def fetch_upsert_and_push(export_url, params, tab_name, unique_keys):
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

    existing_df = get_as_dataframe(worksheet).dropna(how='all').dropna(axis=1, how='all')
    
    if existing_df.empty:
        final_df = df_fresh
        updated_rows = len(final_df)
        print(f"Inisialisasi awal, {updated_rows} baris disiapkan.")
    else:
        missing_cols = [col for col in unique_keys if col not in df_fresh.columns or col not in existing_df.columns]
        if missing_cols:
            print(f"❌ ERROR KRITIS: Kolom unik {missing_cols} tidak ditemukan di Sheet/API.")
            # STANDAR LOG 4: FAILED (KOLOM BERUBAH)
            update_cache_log(tab_name, "Failed (Schema Error - Missing Columns)")
            return

        existing_headers = existing_df.columns.tolist()
        combined_df = pd.concat([existing_df, df_fresh], ignore_index=True)
        final_df = combined_df.drop_duplicates(subset=unique_keys, keep='last')
        final_df = final_df.reindex(columns=existing_headers)
        
        updated_rows = len(final_df) - len(existing_df)
        print(f"Upsert & Column Mapping selesai. Penambahan bersih: {updated_rows} baris.")

    worksheet.clear()
    set_with_dataframe(worksheet, final_df)
    print("✅ Berhasil Diperbarui!\n")
    
    # STANDAR LOG 5: SUCCESSFUL (Berhasil diproses)
    update_cache_log(tab_name, "Successful")


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
else:
    error_msg = "❌ ERROR: Gagal mendapatkan token Fulfillment. Bearer Token kemungkinan kedaluwarsa!"
    print(error_msg)
    update_cache_log("MTD_Fulfillment", "Failed (Token Expired)")

# --- DELIVERY ---
d_token = get_fresh_export_token()
if d_token:
    fetch_upsert_and_push(export_url, {
        "type": "delivery", "warehouseId": WAREHOUSE_ID, "tenantId": TENANT_ID,
        "startDate": start_date, "endDate": end_date, "updateStatus": "false",
        "token": d_token, "id": REQUEST_ID
    }, "MTD_Delivery", ["Kode Pengiriman", "SKU Produk"])
else:
    error_msg = "❌ ERROR: Gagal mendapatkan token Delivery. Bearer Token kemungkinan kedaluwarsa!"
    print(error_msg)
    update_cache_log("MTD_Delivery", "Failed (Token Expired)")

# --- RETURNS ---
r_token = get_fresh_export_token()
if r_token:
    fetch_upsert_and_push(export_url, {
        "type": "return", "warehouseId": WAREHOUSE_ID, "tenantId": TENANT_ID,
        "startDate": start_date, "endDate": end_date,
        "token": r_token, "id": REQUEST_ID
    }, "MTD_Returns", ["Kode Fulfillment", "Kode SKU"])
else:
    error_msg = "❌ ERROR: Gagal mendapatkan token Returns. Bearer Token kemungkinan kedaluwarsa!"
    print(error_msg)
    update_cache_log("MTD_Returns", "Failed (Token Expired)")

print("🎉 SINKRONISASI TUNTAS 100%.")
