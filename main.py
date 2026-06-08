import os
import json
import gspread
import pandas as pd
import requests
import io
import time
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials

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
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36 Edg/149.0.0.0"
}

# ==========================================
# 2. LOGIKA GENERATE EXPORT TOKEN (CRITICAL FIX)
# ==========================================
def get_fresh_export_token():
    url = "https://api.freshfactory.id/v1/admin/exports/generate_token"
    for _ in range(3):
        try:
            response = requests.get(url, headers=HEADERS)
            if response.status_code == 200:
                data = response.json()
                return data.get('data', {}).get('exportToken')
        except Exception as e:
            print("Gagal mengambil token export, mencoba lagi...")
        time.sleep(2)
    return None

# ==========================================
# 3. LOGIKA ROLLING WINDOW (1 Bulan Lalu - Besok)
# ==========================================
today = datetime.now()
end_date = (today + timedelta(days=1)).strftime('%Y-%m-%d')

first_day_this_month = today.replace(day=1)
first_day_last_month = (first_day_this_month - timedelta(days=1)).replace(day=1)
start_date = first_day_last_month.strftime('%Y-%m-%d')

print(f"🔄 Menjalankan Rolling Window: {start_date} s/d {end_date}")

# ==========================================
# 4. FUNGSI UPSERT BERDASARKAN NAMA KOLOM
# ==========================================
def upsert_to_sheet_by_column(api_url, tab_name, unique_keys):
    print(f"--- Memproses {tab_name} ---")
    
    response = requests.get(api_url, headers=HEADERS)
    if response.status_code != 200:
        print(f"❌ Error {response.status_code}: Gagal menarik {tab_name}. Respons: {response.text}")
        return
        
    df_fresh = pd.read_excel(io.BytesIO(response.content))
    sheet = gc.open_by_key(SPREADSHEET_ID).worksheet(tab_name)
    existing_data = sheet.get_all_records()
    existing_headers = sheet.row_values(1)
    
    if not existing_data:
        final_df = df_fresh
        headers_to_push = final_df.columns.tolist()
        print(f"Inisialisasi awal, {len(final_df)} baris disiapkan.")
    else:
        df_existing = pd.DataFrame(existing_data)
        combined = pd.concat([df_existing, df_fresh], ignore_index=True)
        final_df = combined.drop_duplicates(subset=unique_keys, keep='last')
        
        if not existing_headers:
            existing_headers = df_fresh.columns.tolist()
            
        final_df = final_df.reindex(columns=existing_headers)
        headers_to_push = existing_headers
        print(f"Upsert & Column Mapping selesai. Total baris: {len(final_df)}.")
    
    final_df = final_df.fillna("")
    sheet.clear()
    sheet.update([headers_to_push] + final_df.values.tolist())
    print(f"✅ Data {tab_name} berhasil di-push sesuai format kolom Sheet!\n")

# ==========================================
# 5. EKSEKUSI PIPELINE
# ==========================================
base_url = "https://api.freshfactory.id/v1/admin/exports"
export_token = get_fresh_export_token()

if not export_token:
    print("❌ Gagal mendapatkan token export (generate_token). Pastikan BEARER_TOKEN Anda di GitHub Secrets masih aktif!")
    exit(1)

# Eksekusi Fulfillment
url_fulfillment = f"{base_url}?type=fulfillment_detail&warehouse_id={WAREHOUSE_ID}&tenant_id={TENANT_ID}&start_order_date_in={start_date}&end_order_date_in={end_date}&page=1&limit=15&token={export_token}&id={REQUEST_ID}"
upsert_to_sheet_by_column(url_fulfillment, "MTD_Fulfillment", ["Kode Fulfillment", "SKU Produk"])

# Eksekusi Delivery
url_delivery = f"{base_url}?type=delivery&warehouseId={WAREHOUSE_ID}&tenantId={TENANT_ID}&startDate={start_date}&endDate={end_date}&updateStatus=false&token={export_token}&id={REQUEST_ID}"
upsert_to_sheet_by_column(url_delivery, "MTD_Delivery", ["Kode Pengiriman", "SKU Produk"])

# Eksekusi Returns
url_returns = f"{base_url}?type=return&warehouseId={WAREHOUSE_ID}&tenantId={TENANT_ID}&startDate={start_date}&endDate={end_date}&token={export_token}&id={REQUEST_ID}"
upsert_to_sheet_by_column(url_returns, "MTD_Returns", ["Kode Fulfillment", "Kode SKU"])

print("🎉 SINKRONISASI TUNTAS 100%.")
