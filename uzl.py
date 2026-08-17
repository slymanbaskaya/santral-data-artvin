# -*- coding: utf-8 -*-

import os
from dotenv import load_dotenv
load_dotenv()
import json
import requests
import pandas as pd
import time
import calendar
from datetime import datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine, text

# =====================================================
# KULLANICI BILGILERI
# =====================================================
EPIAS_USER = os.getenv("EPIAS_USER")  # ⚠️ PROD: ENV kullanın
EPIAS_PASS = "Maslak127."  # ⚠️ PROD: ENV kullanın

# =====================================================
# VERITABANI AYARLARI (GCP)
# =====================================================
# 1. Veritabanı Bağlantı Bilgileri (Aiven)
GCP_HOST = os.getenv("GCP_HOST", "enerstra-enerstra.h.aivencloud.com")
GCP_DB = os.getenv("GCP_DB", "enerstra3_db")
GCP_USER = os.getenv("GCP_USER", "avnadmin")
GCP_PASSWORD = os.getenv("GCP_PASSWORD")
GCP_PORT = os.getenv("GCP_PORT", "16505")

TABLE_NAME = "santral_data_artvin_uzl"

# =====================================================
# SERVIS AYARLARI
# =====================================================
CAS_TICKETS_URL = "https://cas.epias.com.tr/cas/v1/tickets"
SERVICE_URL = "https://epys.epias.com.tr/reconciliation-invoice/v1/reconciliation/detail/hourly"
USER_AGENT = "DogusEnerji-ReconciliationInvoice/1.0"

SESSION = requests.Session()
SESSION.trust_env = False

def get_tgt():
    response = SESSION.post(
        CAS_TICKETS_URL,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "text/plain",
            "User-Agent": USER_AGENT,
        },
        data={
            "username": EPIAS_USER,
            "password": EPIAS_PASS,
        },
        timeout=30,
    )

    tgt = response.text.strip()
    if response.status_code not in (200, 201) or not tgt.startswith("TGT-"):
        raise RuntimeError(f"TGT alinamadi. HTTP {response.status_code}: {response.text}")
    
    print("TGT basariyla alindi.")
    return tgt

def generate_monthly_periods(start_year=2026, start_month=1):
    """2026 başından, uzlaştırması kesinleşmiş aya kadar aylık periyotları hesaplar."""
    periods = []
    now = datetime.now()

    current = datetime(start_year, start_month, 1)

    # EPİAŞ uzlaştırma açıklanma kuralı kontrolü
    if now.day < 15:
        # Ayın 15'inden önceyse, sadece 2 ay öncesinin verisi kesinleşmiştir.
        end = datetime(now.year, now.month, 1) - relativedelta(months=2)
    else:
        # Ayın 15'i ve sonraysa, 1 ay öncesinin verisi kesinleşmiştir.
        end = datetime(now.year, now.month, 1) - relativedelta(months=1)

    while current <= end:
        last_day = calendar.monthrange(current.year, current.month)[1]

        start_str = f"{current.year}-{current.month:02d}-01T00:00:00+03:00"
        end_str = f"{current.year}-{current.month:02d}-{last_day:02d}T23:00:00+03:00"

        periods.append({
            "period": start_str,
            "version": start_str,
            "effectiveDateStart": start_str,
            "effectiveDateEnd": end_str,
            "region": "TR1",
            "page": {
                "number": 1,
                "size": 1000
            }
        })

        current += relativedelta(months=1)

    return periods

def check_data_exists(engine, period_start_str):
    """Veritabanında ilgili aya ait verinin zaten olup olmadığını kontrol eder."""
    # "2026-03-01T00:00:00+03:00" formatından "2026-03" kısmını alıyoruz
    period_ym = period_start_str[0:7]
    
    query = f"""
    SELECT 1 
    FROM {TABLE_NAME} 
    WHERE TO_CHAR(datetime, 'YYYY-MM') = '{period_ym}' 
    LIMIT 1
    """
    try:
        df = pd.read_sql(query, engine)
        return not df.empty
    except Exception:
        # Tablo henüz oluşturulmamışsa hata verir, bu durumda veri yoktur diyoruz
        return False

def find_data_list(value):
    if isinstance(value, list):
        if not value or isinstance(value[0], dict):
            return value
        return None

    if isinstance(value, dict):
        preferred_keys = (
            "items", "content", "data", "list", "result", "rows",
            "details", "detailList", "reconciliationDetailList",
            "reconciliationSummaryHourlyList",
        )
        for key in preferred_keys:
            candidate = value.get(key)
            if isinstance(candidate, list):
                return candidate
        for candidate in value.values():
            found = find_data_list(candidate)
            if found is not None:
                return found
    return None

def standardize_columns(df):
    """
    1. İstenmeyen sütunları (version, region) veri çerçevesinden çıkarır.
    2. Zaman belirten sütunu 'datetime' olarak günceller.
    3. Tabloda mükerrer sütun oluşmasını engellemek için Enerstra yeni şemasına eşleme yapar.
    """
    column_mapping = {
        "date": "datetime",
        "period": "datetime",
        "zaman": "datetime",
        "effectiveDate": "datetime", 
        "effectiveDateStart": "datetime",
        "pozitifEdmDsgMwh": "positiveBalanceGroupImbalanceVolume",
        "pozitifEdtDsgTl": "positiveBalanceGroupImbalanceAmount",
        "negatifEdmDsgMwh": "negativeBalanceGroupImbalanceVolume",
        "negatifEdtDsgTl": "negativeBalanceGroupImbalanceAmount"
    }
    
    df.rename(columns=column_mapping, inplace=True)
    
    cols_to_drop = [col for col in ["version", "region"] if col in df.columns]
    if cols_to_drop:
        df.drop(columns=cols_to_drop, inplace=True)
        
    return df

def save_to_db(response_json, engine):
    rows = find_data_list(response_json)

    if not rows:
        print("Uyari: Servis basarili yanit verdi ancak veri listesi bos geldi. Atlanıyor.")
        return 0

    df = pd.json_normalize(rows)
    df = standardize_columns(df)

    # Veritabanina yazma islemi
    df.to_sql(TABLE_NAME, engine, if_exists="append", index=False)
    
    return len(df)

def main():
    # GCP PostgreSQL baglantisini olustur
    engine = create_engine(f"postgresql://{GCP_USER}:{GCP_PASSWORD}@{GCP_HOST}:{GCP_PORT}/{GCP_DB}")
    
    try:
        tgt = get_tgt()
    except Exception as e:
        print(e)
        return

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
        "TGT": tgt,
        "Cache-Control": "no-cache",
    }

    periods = generate_monthly_periods(2026, 1)
    
    print(f"\nToplam {len(periods)} aylik periyot takvime alindi. Veritabani kontrolleri basliyor...")

    for payload in periods:
        period_text = payload["period"][0:7]
        
        # API'ye gitmeden önce veritabanında bu ayın verisi var mı diye kontrol et
        if check_data_exists(engine, payload["period"]):
            print(f"[{period_text}] donemi zaten veritabaninda mevcut. Atlaniyor...")
            continue
            
        print(f"\n[{period_text}] donemi icin EPİAŞ'tan veri getiriliyor...")

        response = SESSION.post(
            SERVICE_URL,
            headers=headers,
            json=payload,
            timeout=120,
        )

        if response.status_code != 200:
            print(f"Hata: {period_text} donemi icin API {response.status_code} dondurdu.")
            continue

        try:
            response_json = response.json()
            inserted_rows = save_to_db(response_json, engine)
            print(f"[{period_text}] - {inserted_rows} satir veritabanina kaydedildi.")
        except Exception as e:
            print(f"[{period_text}] veri islenirken hata olustu: {e}")

        # Servise ardisik yuklenmemek icin kucuk bir bekleme
        time.sleep(2)

    print("\nTum islemler tamamlandi.")

if __name__ == "__main__":
    main()