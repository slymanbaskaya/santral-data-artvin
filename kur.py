# -*- coding: utf-8 -*-
import os
from dotenv import load_dotenv
load_dotenv()
import re
import json
import unicodedata
import requests
import pandas as pd
import psycopg2
import xml.etree.ElementTree as ET
from psycopg2 import sql
from psycopg2.extras import execute_values
from datetime import datetime, date, time, timedelta

# 1. Veritabanı Bağlantı Bilgileri (Aiven)
GCP_HOST = os.getenv("GCP_HOST", "enerstra-enerstra.h.aivencloud.com")
GCP_DB = os.getenv("GCP_DB", "enerstra3_db")
GCP_USER = os.getenv("GCP_USER", "avnadmin")
GCP_PASSWORD = os.getenv("GCP_PASSWORD")
GCP_PORT = os.getenv("GCP_PORT", "16505")

TABLE_NAME = "tcmb_usd_kur_h"
TARGET_DATETIME_COL = "datetime"
FALLBACK_START = "2026-01-01"
CHUNK_DAYS = 31
LOOKBACK_DAYS = 10
GLOBAL_FETCH_FLOOR = date(2005, 1, 1)
TIMEOUT = 30

UA_HDRS = {"User-Agent": "Mozilla/5.0 (compatible; TCMB-Delta-Hourly-GCP/1.0)"}
CAS_TGT_URL = "https://giris.epias.com.tr/cas/v1/tickets"

def tcmb_url_for(d: date) -> str:
    return f"https://www.tcmb.gov.tr/kurlar/{d:%Y%m}/{d:%d%m%Y}.xml"

def build_retry_session(total=3, backoff_factor=1):
    try:
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        sess = requests.Session()
        retry = Retry(total=total, connect=total, read=total, backoff_factor=backoff_factor,
                      status_forcelist=(429, 500, 502, 503, 504), allowed_methods=frozenset(["GET", "POST"]), raise_on_status=False)
        sess.mount("https://", HTTPAdapter(max_retries=retry))
        sess.mount("http://", HTTPAdapter(max_retries=retry))
        return sess
    except Exception:
        return requests.Session()

SESSION = build_retry_session(total=3, backoff_factor=1)

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
    """ Zaman sütununu 'datetime' yapar, en başa taşır ve diğer sütunları temizler. """
    df = df.copy()
    df.columns = [sanitize_col(c) for c in df.columns]
    other_cols = [c for c in df.columns if c != TARGET_DATETIME_COL]
    ordered_cols = [TARGET_DATETIME_COL] + other_cols
    return df[ordered_cols]

def iter_periods(start_d: date, end_d: date, step_days: int):
    cur = start_d
    one, step = timedelta(days=1), timedelta(days=step_days - 1)
    while cur <= end_d:
        chunk_end = min(cur + step, end_d)
        yield cur.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")
        cur = chunk_end + one

def fetch_usd_forexbuying(d: date):
    try:
        r = SESSION.get(tcmb_url_for(d), timeout=TIMEOUT, headers=UA_HDRS)
        if r.status_code != 200: return None
        root = ET.fromstring(r.content)
        for cur in root.findall("Currency"):
            if (cur.get("Kod") or "").upper() == "USD":
                node = cur.find("ForexBuying")
                if node is not None and (node.text or "").strip():
                    return float(node.text.strip().replace(",", "."))
        return None
    except Exception:
        return None

def expand_daily_to_hourly(df_daily: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, r in df_daily.iterrows():
        g, v = r["date"], r["usd_buying"]
        hours = pd.date_range(g.normalize(), periods=24, freq="h")
        for h in hours:
            rows.append({TARGET_DATETIME_COL: h, "usd_buying": v})
    return pd.DataFrame(rows)

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
            cur.execute(sql.SQL("SELECT MAX({}) FROM {}").format(sql.Identifier(TARGET_DATETIME_COL), sql.Identifier(TABLE_NAME)))
            row = cur.fetchone()
            return row[0].date() if row and row[0] else None
    except Exception:
        return None

def get_last_rate_before(dt_val: datetime) -> float | None:
    """ Verilen datetime değerinden önceki son kuru veritabanından çeker (ffill tohumlaması için). """
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s", (TABLE_NAME,))
            cols = {row[0] for row in cur.fetchall()}
            if TARGET_DATETIME_COL not in cols: return None
            
            usd_col = next((c for c in cols if "usd_buying" in c.lower()), "usd_buying")
            query = sql.SQL("SELECT {} FROM {} WHERE {} < %s ORDER BY {} DESC LIMIT 1").format(
                sql.Identifier(usd_col), sql.Identifier(TABLE_NAME), sql.Identifier(TARGET_DATETIME_COL), sql.Identifier(TARGET_DATETIME_COL)
            )
            cur.execute(query, (dt_val,))
            row = cur.fetchone()
            return float(row[0]) if row and row[0] is not None else None
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
def delta_update():
    effective_end_day = date.today() + timedelta(days=1)
    print(f"Etkin bitiş, yani dün: {effective_end_day:%Y-%m-%d}")

    last_effective = get_gcp_max_date()
    if last_effective:
        effective_start_day = last_effective + timedelta(days=1)
        print(f"Tablodaki son tarih (datetime): {last_effective:%Y-%m-%d} -> Eksik etkin başlangıç: {effective_start_day:%Y-%m-%d}")
    else:
        effective_start_day = datetime.strptime(FALLBACK_START, "%Y-%m-%d").date()
        print(f"Tablo boş/görülemiyor. Etkin başlangıç: {effective_start_day:%Y-%m-%d}")

    fallback_dt = datetime.strptime(FALLBACK_START, "%Y-%m-%d").date()
    if effective_start_day < fallback_dt:
        effective_start_day = fallback_dt

    if effective_start_day > effective_end_day:
        print("✓ TCMB kur tablosu zaten güncel.")
        return

    total_rows = 0
    for eff_chunk_start_str, eff_chunk_end_str in iter_periods(effective_start_day, effective_end_day, CHUNK_DAYS):
        eff_chunk_start = datetime.strptime(eff_chunk_start_str, "%Y-%m-%d").date()
        eff_chunk_end = datetime.strptime(eff_chunk_end_str, "%Y-%m-%d").date()
        print(f"\n-> Etkin dilim: {eff_chunk_start} .. {eff_chunk_end}")

        bul_fetch_start = max(eff_chunk_start - timedelta(days=1 + LOOKBACK_DAYS), GLOBAL_FETCH_FLOOR)
        bul_fetch_end = eff_chunk_end - timedelta(days=1)

        if bul_fetch_end < bul_fetch_start:
            print("  * Bu dilimde bülten aralığı boş, atlanıyor.")
            continue

        bul_idx = pd.date_range(bul_fetch_start, bul_fetch_end, freq="D")
        df_bul = pd.DataFrame(index=bul_idx, data={"usd_buying": pd.NA})

        for dts in bul_idx:
            val = fetch_usd_forexbuying(dts.date())
            if val is not None:
                df_bul.loc[dts, "usd_buying"] = val

        df_bul = df_bul.dropna(subset=["usd_buying"]).reset_index().rename(columns={"index": "bulletin_date"})
        if df_bul.empty:
            print("  * Bu bülten aralığında veri yok, atlanıyor.")
            continue

        df_bul["effective_date"] = pd.to_datetime(df_bul["bulletin_date"]) + pd.Timedelta(days=1)
        eff_idx_ext_start = max(eff_chunk_start - timedelta(days=LOOKBACK_DAYS), GLOBAL_FETCH_FLOOR)
        eff_idx_ext = pd.date_range(eff_idx_ext_start, eff_chunk_end, freq="D")
        df_eff = pd.DataFrame(index=eff_idx_ext, data={"usd_buying": pd.NA})

        for _, r in df_bul.iterrows():
            eff_date = pd.to_datetime(r["effective_date"]).normalize()
            if eff_date in df_eff.index:
                df_eff.loc[eff_date, "usd_buying"] = r["usd_buying"]

        seed_dt = datetime.combine(eff_chunk_start, time(0, 0, 0))
        seed_val = get_last_rate_before(seed_dt)

        if pd.isna(df_eff["usd_buying"].iloc[0]) and seed_val is not None:
            df_eff.iloc[0, df_eff.columns.get_loc("usd_buying")] = seed_val

        df_eff["usd_buying"] = pd.to_numeric(df_eff["usd_buying"], errors="coerce").ffill()

        eff_idx_write = pd.date_range(eff_chunk_start, eff_chunk_end, freq="D")
        df_daily = df_eff.loc[eff_idx_write].dropna(subset=["usd_buying"]).reset_index().rename(columns={"index": "date"})
        df_daily["date"] = pd.to_datetime(df_daily["date"], errors="coerce")
        df_daily["usd_buying"] = pd.to_numeric(df_daily["usd_buying"], errors="coerce")

        df_hourly = expand_daily_to_hourly(df_daily)
        df_hourly = normalize_df_columns(df_hourly)

        print(f"  {len(df_daily)} etkin gün -> {len(df_hourly)} saatlik kayıt.")
        if df_hourly.empty:
            print("  * Yazılacak saatlik veri yok, atlanıyor.")
            continue

        s_dt = datetime.combine(eff_chunk_start, time(0, 0, 0))
        e_dt = datetime.combine(eff_chunk_end, time(23, 59, 59))

        write_range_to_gcp(df_hourly, s_dt, e_dt)
        total_rows += len(df_hourly)
        print(f"  ✓ Dilim yazıldı. Kümülatif saatlik satır: {total_rows}")

    print(f"\n✓ TCMB Kur işlemi tamamlandı. Toplam yazılan yeni saatlik satır: {total_rows}")

if __name__ == "__main__":
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT current_database(), NOW();")
        print(f"✓ GCP PostgreSQL bağlantısı başarılı: {cur.fetchone()}")

    print("TCMB kur delta akışı başlatılıyor...")
    delta_update()