# -*- coding: utf-8 -*-
import os
from dotenv import load_dotenv
load_dotenv()
import json
import re
import unicodedata
import requests
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from datetime import datetime, date, time, timedelta
# SSL/TLS uyumluluğu için gerekli paketler
from requests.adapters import HTTPAdapter
from urllib3.util import SSLContext
import ssl

# ==============================
# 0) ÖZEL TLS/SSL UYUMLULUK ADAPTÖRÜ
# ==============================
class DESAdapter(HTTPAdapter):
    """
    Cloud Run container'ları ile EPİAŞ CAS sunucusu arasındaki 
    şifreleme (Cipher Suite) uyumsuzluğunu çözen adaptör.
    """
    def init_poolmanager(self, *args, **kwargs):
        context = SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        # EPİAŞ'ın el sıkışmada (handshake) beklediği esnek şifreleme havuzunu zorluyoruz
        context.set_ciphers('DEFAULT:@SECLEVEL=1:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES128-GCM-SHA256')
        context.load_default_certs()
        kwargs['ssl_context'] = context
        return super(DESAdapter, self).init_poolmanager(*args, **kwargs)

# ==============================
# 1) AYARLAR VE SABİTLER
# ==============================
EPIAS_EMAIL     = os.getenv("EPIAS_EMAIL")
EPIAS_PASSWORD  = os.getenv("EPIAS_PASSWORD")

# 1. Veritabanı Bağlantı Bilgileri (Aiven)
GCP_HOST = os.getenv("GCP_HOST", "enerstra-enerstra.h.aivencloud.com")
GCP_DB = os.getenv("GCP_DB", "enerstra3_db")
GCP_USER = os.getenv("GCP_USER", "avnadmin")
GCP_PASSWORD = os.getenv("GCP_PASSWORD")
GCP_PORT = os.getenv("GCP_PORT", "16505")

TABLE_NAME = "epias_ptf_smf_sdf"
TARGET_DATETIME_COL = "datetime"
FALLBACK_START = "2026-01-01"
CHUNK_DAYS = 31
TIMEOUT = 15  # Bulut ortamında gereksiz kilitlenmeleri önlemek için 15 saniyeye düşürüldü

DATA_URL = "https://seffaflik.epias.com.tr/reporting-service/v1/data/ptf-smf-sdf"
CAS_TGT_URL = "https://giris.epias.com.tr/cas/v1/tickets"

# ==============================
# 2) YARDIMCI FONKSİYONLAR
# ==============================
def get_conn():
    return psycopg2.connect(host=GCP_HOST, database=GCP_DB, user=GCP_USER, password=GCP_PASSWORD, port=GCP_PORT, connect_timeout=30)

def sanitize_col(name: str) -> str:
    s = str(name).strip().translate(str.maketrans("ıçğöşüİÇĞÖŞÜ", "icgosuICGOSU"))
    s = ''.join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    s = re.sub(r"[^A-Za-z0-9_]", "_", s)
    return f"f_{s}" if not s or s[0].isdigit() else s

def normalize_df_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [sanitize_col(c) for c in df.columns]
    
    if "date" in df.columns:
        parsed = pd.to_datetime(df["date"], errors="coerce")
        if isinstance(parsed.dtype, pd.DatetimeTZDtype):
            df[TARGET_DATETIME_COL] = parsed.dt.tz_localize(None)
        else:
            df[TARGET_DATETIME_COL] = parsed
        df = df.drop(columns=["date"])
        
    if "time" in df.columns:
        df = df.drop(columns=["time"])
        
    if TARGET_DATETIME_COL not in df.columns:
        df[TARGET_DATETIME_COL] = pd.NaT

    other_cols = [c for c in df.columns if c != TARGET_DATETIME_COL]
    ordered_cols = [TARGET_DATETIME_COL] + other_cols
    
    return df[ordered_cols]

def get_tgt(email: str, password: str) -> str:
    # Bağlantıyı yönetecek ve adaptörü taşıyacak session mimarisi
    session = requests.Session()
    session.mount('https://giris.epias.com.tr', DESAdapter())
    
    try:
        r = session.post(CAS_TGT_URL, data={"username": email, "password": password}, headers={"Accept": "text/plain"}, timeout=TIMEOUT)
        if r.status_code == 201 and r.text.strip():
            return r.text.strip()
        raise RuntimeError(f"TGT alınamadı: HTTP {r.status_code}")
    except requests.exceptions.SSLError as e:
        raise RuntimeError(f"EPİAŞ SSL Bağlantı Sorunu (Bulut El Sıkışma Hatası): {e}")

def fetch_all_pages(start_date: str, end_date: str, tgt: str, page_size: int = 10000) -> list:
    all_items, page_no = [], 1
    headers = {"Content-Type": "application/json", "TGT": tgt}
    
    while True:
        payload = {
            "startDate": f"{start_date}T00:00:00+03:00",
            "endDate":   f"{end_date}T23:59:59+03:00",
            "page": {"number": page_no, "size": page_size, "sort": {"direction": "ASC", "field": "date"}}
        }
        r = requests.post(DATA_URL, headers=headers, data=json.dumps(payload), timeout=TIMEOUT)
        
        if r.status_code in (400, 422):
            raise ValueError(f"EPİAŞ Gelecek Tarih Kısıtı: HTTP {r.status_code} (Yarının PTF/SMF verileri henüz ilan edilmemiş)")
        if r.status_code != 200:
            raise RuntimeError(f"/v1/data/ptf-smf-sdf hata: HTTP {r.status_code}")
        
        data = r.json()
        items = next((data[k] for k in ("items", "content", "data", "result") if k in data and isinstance(data[k], list)), None)
        if items is None and "items" in data and isinstance(data["items"], dict):
            items = data["items"].get("content", [])
            
        items = items or []
        all_items.extend(items)
        if len(items) < page_size: break
        page_no += 1
        
    return all_items

# ==============================
# 3) VERİTABANI İŞLEMLERİ
# ==============================
def ensure_table(conn, schema: dict):
    with conn.cursor() as cur:
        defs = [sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL(t)) for c, t in schema.items()]
        cur.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(sql.Identifier(TABLE_NAME), sql.SQL(", ").join(defs)))
        
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s", (TABLE_NAME,))
        existing = {row[0] for row in cur.fetchall()}
        for c, t in schema.items():
            if c not in existing:
                cur.execute(sql.SQL("ALTER TABLE {} ADD COLUMN {} {}").format(sql.Identifier(TABLE_NAME), sql.Identifier(c), sql.SQL(t)))
    conn.commit()

def get_gcp_max_date() -> date | None:
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name = %s AND column_name = %s)", (TABLE_NAME, TARGET_DATETIME_COL))
            if not cur.fetchone()[0]: return None
            
            cur.execute("SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name = %s AND column_name = 'smf')", (TABLE_NAME,))
            if cur.fetchone()[0]:
                cur.execute(sql.SQL("SELECT MAX({}) FROM {} WHERE smf IS NOT NULL").format(sql.Identifier(TARGET_DATETIME_COL), sql.Identifier(TABLE_NAME)))
            else:
                cur.execute(sql.SQL("SELECT MAX({}) FROM {}").format(sql.Identifier(TARGET_DATETIME_COL), sql.Identifier(TABLE_NAME)))
                
            row = cur.fetchone()
            return row[0].date() if row and row[0] else None
    except Exception:
        return None

def write_range_to_gcp(df: pd.DataFrame, start_dt: datetime, end_dt: datetime):
    if df.empty: return

    schema = {}
    for c in df.columns:
        if c == TARGET_DATETIME_COL or pd.api.types.is_datetime64_any_dtype(df[c]): schema[c] = "TIMESTAMP WITHOUT TIME ZONE"
        elif pd.api.types.is_bool_dtype(df[c]): schema[c] = "BOOLEAN"
        elif pd.api.types.is_numeric_dtype(df[c]): schema[c] = "DOUBLE PRECISION"
        else: schema[c] = "TEXT"

    with get_conn() as conn:
        ensure_table(conn, schema)
        with conn.cursor() as cur:
            cur.execute(sql.SQL("DELETE FROM {} WHERE {} BETWEEN %s AND %s").format(sql.Identifier(TABLE_NAME), sql.Identifier(TARGET_DATETIME_COL)), (start_dt, end_dt))
            
            ins_sql = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(sql.Identifier(TABLE_NAME), sql.SQL(", ").join(sql.Identifier(c) for c in df.columns))
            values = [tuple(None if pd.isna(v) else (v.to_pydatetime() if isinstance(v, pd.Timestamp) else (v.item() if hasattr(v, "item") else v)) for v in row) for row in df.values]
            execute_values(cur, ins_sql.as_string(conn), values, page_size=5000)
        conn.commit()

# ==============================
# 4) DELTA AKIŞI VE ANA ÇALIŞTIRICI
# ==============================
def delta_update(tgt: str):
    end_day = date.today() + timedelta(days=1)
    
    last_db_date = get_gcp_max_date()
    if last_db_date:
        start_day = last_db_date - timedelta(days=1)
    else:
        start_day = datetime.strptime(FALLBACK_START, "%Y-%m-%d").date()
        
    start_day = max(start_day, datetime.strptime(FALLBACK_START, "%Y-%m-%d").date())

    if start_day > end_day:
        print(f"✓ {TABLE_NAME} zaten güncel.")
        return

    print(f"-> PTF/SMF/SDF Güncelleme aralığı: {start_day} .. {end_day}")
    total_rows, cur_day = 0, start_day

    while cur_day <= end_day:
        chunk_end = min(cur_day + timedelta(days=CHUNK_DAYS - 1), end_day)
        c_start_str, c_end_str = cur_day.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")
        print(f"  Dilim çekiliyor: {c_start_str} .. {c_end_str}")

        try:
            items = fetch_all_pages(c_start_str, c_end_str, tgt)
        except ValueError as ve:
            print(f"  ℹ Bilgi: {ve}. Akış mevcut en son güncel veride durduruldu.")
            break
        except RuntimeError as e:
            print(f"  Büyük veri hacmi veya hata! Sayfa boyutu düşürülerek tekrar deneniyor... Hata: {e}")
            try:
                items = fetch_all_pages(c_start_str, c_end_str, tgt, page_size=5000)
            except ValueError as ve:
                print(f"  ℹ Bilgi: {ve}. Akış mevcut en son güncel veride durduruldu.")
                break

        if items:
            df = pd.json_normalize(items, sep="_")
            df = normalize_df_columns(df)
            
            s_dt = datetime.combine(cur_day, time(0, 0, 0))
            e_dt = datetime.combine(chunk_end, time(23, 59, 59))
            
            write_range_to_gcp(df, s_dt, e_dt)
            total_rows += len(df)
            print(f"  ✓ {len(df)} satır yazıldı.")

        cur_day = chunk_end + timedelta(days=1)
    print(f"\n✓ PTF/SMF/SDF işlemi tamamlandı. Toplam yeni yazılan satır: {total_rows}")

if __name__ == "__main__":
    if "YAZ" in [EPIAS_EMAIL, EPIAS_PASSWORD, GCP_PASSWORD]: raise SystemExit("Lütfen kimlik bilgilerini eksiksiz girin.")
    
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT current_database(), NOW();")
        print(f"✓ GCP PostgreSQL bağlantısı başarılı: {cur.fetchone()}")

    print("TGT alınıyor ve delta akışı başlatılıyor...")
    delta_update(get_tgt(EPIAS_EMAIL, EPIAS_PASSWORD))