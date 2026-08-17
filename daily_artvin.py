import os
import re
import uuid
import time
import requests
import pandas as pd
from io import StringIO
from itertools import groupby
from datetime import datetime, date, timedelta, timezone
from sqlalchemy import create_engine, text
from dotenv import load_dotenv  # <-- Bunu ekleyin

# .env dosyasını yükle
load_dotenv()

# ==========================
# VERİTABANI AYARLARI (GCP)
# ==========================
GCP_HOST = os.getenv("GCP_HOST")
GCP_DB = os.getenv("GCP_DB")
GCP_USER = os.getenv("GCP_USER")
GCP_PASSWORD = os.getenv("GCP_PASSWORD")
GCP_PORT = os.getenv("GCP_PORT")
TABLE_NAME = "santral_data_artvin_ham"

# ==========================
# EPİAŞ AYARLARI
# ==========================
EPIAS_USER = os.getenv("EPIAS_USER")
EPIAS_PASS = os.getenv("EPIAS_PASS")

SEFFAFLIK_USER = os.getenv("SEFFAFLIK_USER")
SEFFAFLIK_PASS = os.getenv("SEFFAFLIK_PASS")

START_DATE = date(2026, 1, 1)
END_DATE = (datetime.now(timezone(timedelta(hours=3))).date() - timedelta(days=1))

USER_AGENT = "DogusEnerji-Konsolide-Aylik/2.6"
CAS_URL = "https://cas.epias.com.tr/cas/v1/tickets"
CAS_GIRIS_URL = "https://giris.epias.com.tr/cas/v1/tickets"
GIP_URL = "https://gunici.epias.com.tr/gunici-service/rest/v1/match/by-organization"
GOP_HOST = "https://gop.epias.com.tr"
GOP_ENDPOINT = f"{GOP_HOST}/gop-servis/rest/offer/offerresult"
REALTIME_URL = "https://seffaflik.epias.com.tr/electricity-service/v1/generation/export/realtime-generation"
BPM_URL = "https://epys.epias.com.tr/reconciliation-bpm/v1/instruction/list"

PLANT_ID = 1974  # Artvin

FINAL_COLUMNS = [
    "datetime", "damSalesVolume", "damSalesAmount", "damPurchasesVolume", "damPurchaseAmount",
    "bcSalesVolume", "bcPurchaseVolume", "idmSalesVolume", "idmSalesAmount", "idmPurchasesVolume",
    "idmPurchaseAmount", "efmSalesVolume", "efmPurchaseVolume", "acceptedUpRegulation",
    "acceptedUpRegulationAmount", "acceptedDownRegulation", "acceptedDownRegulationAmount",
    "kupsm", "kupst", "generation", "consumption", "positiveBalanceGroupImbalanceVolume",
    "positiveBalanceGroupImbalanceAmount", "negativeBalanceGroupImbalanceVolume", "negativeBalanceGroupImbalanceAmount"
]

def get_db_engine():
    conn_str = f"postgresql+psycopg2://{GCP_USER}:{GCP_PASSWORD}@{GCP_HOST}:{GCP_PORT}/{GCP_DB}"
    return create_engine(conn_str)

def get_missing_days(engine, start_d, end_d):
    try:
        query = f"""
            SELECT DATE(datetime) as d, COUNT(*) as c 
            FROM {TABLE_NAME} 
            WHERE datetime >= '{start_d}' AND datetime < '{end_d + timedelta(days=1)}'
            GROUP BY DATE(datetime)
        """
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
            complete_days = set(df[df['c'] >= 24]['d'])
    except Exception:
        complete_days = set()

    all_days = {start_d + timedelta(days=i) for i in range((end_d - start_d).days + 1)}
    return sorted(list(all_days - complete_days))

def delete_days_from_db(engine, start_d, end_d):
    query = f"DELETE FROM {TABLE_NAME} WHERE DATE(datetime) >= '{start_d.strftime('%Y-%m-%d')}' AND DATE(datetime) <= '{end_d.strftime('%Y-%m-%d')}'"
    try:
        with engine.begin() as conn:
            conn.execute(text(query))
    except Exception:
        pass

def get_tgt(session, user, password, accept_type="application/json", cas_url=CAS_URL):
    print(f"  [Auth Log] {cas_url} adresine TGT isteği atılıyor (Kullanıcı: {user})...")
    r = session.post(
        cas_url,
        headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": accept_type, "User-Agent": USER_AGENT},
        data={"username": user, "password": password},
        timeout=30
    )
    print(f"  [Auth Log] HTTP Status: {r.status_code}")
    if r.status_code not in (200, 201): return None
    
    if accept_type == "application/json":
        try:
            return r.json().get("tgt") or r.json().get("TGT") or r.json().get("ticket")
        except:
            return None
    return r.text.strip() if r.text.strip().startswith("TGT-") else None

def get_st(session, tgt, service_url):
    r = session.post(f"{CAS_URL}/{tgt}", headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "text/plain", "User-Agent": USER_AGENT}, data={"service": service_url}, timeout=30)
    return r.text.strip() if r.status_code in (200, 201) else None

def fetch_gip_period(session, tgt, start_d, end_d):
    print(f"  [GİP Log] {start_d} / {end_d} arası çekiliyor...")
    all_matches = []
    page = 1
    while True:
        payload = {
            "effectiveDateStart": f"{start_d.strftime('%Y-%m-%d')} 00:00:00",
            "effectiveDateEnd": f"{end_d.strftime('%Y-%m-%d')} 23:59:00",
            "pageInfo": {"page": page, "size": 1000},
            "region": "TR1"
        }
        r = session.post(GIP_URL, headers={"TGT": tgt, "Content-Type": "application/json", "Accept": "application/json"}, json=payload)
        if r.status_code != 200: break
        try:
            j = r.json()
            matches = j.get("body", {}).get("content", {}).get("matchesByOrganization", []) if isinstance(j, dict) else []
        except: break
        if not matches: break
        all_matches.extend(matches)
        if len(matches) < 1000: break
        page += 1
        time.sleep(0.2)

    print(f"  [GİP Log] Toplam {len(all_matches)} eşleşme bulundu.")
    if not all_matches: return pd.DataFrame(columns=["datetime", "idmPurchasesVolume", "idmPurchaseAmount", "idmSalesVolume", "idmSalesAmount"])

    df = pd.json_normalize(all_matches)
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce") / 10.0
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["amount"] = df["quantity"] * df["price"]
    
    def parse_dt(c):
        m = re.search(r"(\d{2})(\d{2})(\d{2})(\d{2})", str(c))
        if m: return datetime(2000+int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4)))
        return pd.NaT

    df["datetime"] = df["contract.name"].apply(parse_dt)
    alis = df[df["offerType.value"].str.lower() == "alış"].groupby("datetime").agg(idmPurchasesVolume=("quantity", "sum"), idmPurchaseAmount=("amount", "sum")).reset_index()
    satis = df[df["offerType.value"].str.lower() == "satış"].groupby("datetime").agg(idmSalesVolume=("quantity", "sum"), idmSalesAmount=("amount", "sum")).reset_index()
    return pd.merge(alis, satis, on="datetime", how="outer").fillna(0.0)

def fetch_gop_period(session, tgt, start_d, end_d):
    print(f"  [GOP Log] {start_d} / {end_d} arası çekiliyor...")
    all_rows = []
    
    curr_d = start_d
    while curr_d <= end_d:
        st = get_st(session, tgt, GOP_HOST)
        
        req_tr1 = {
            "header": [{"key": "transactionId", "value": str(uuid.uuid4())}, {"key": "application", "value": "DOGUS_ENERJI_UYGULAMA"}, {"key": "language", "value": "TR"}],
            "body": {"deliveryDay": f"{curr_d.strftime('%Y-%m-%d')}T00:00:00.000+0300", "region": "TR1"}
        }
        
        r = session.post(GOP_ENDPOINT, headers={"gop-service-ticket": st, "Content-Type": "application/json"}, json=req_tr1)
        rows_found = False
        
        if r.status_code == 200:
            try:
                j = r.json()
                if isinstance(j, dict) and str(j.get("resultCode")) == "0":
                    rows = j.get("body", {}).get("optimizationSummaryByOrganizations", [])
                    if rows: 
                        all_rows.extend(rows)
                        rows_found = True
            except Exception:
                pass
        
        if not rows_found:
            st2 = get_st(session, tgt, GOP_HOST)
            req_no_region = {
                "header": [{"key": "transactionId", "value": str(uuid.uuid4())}, {"key": "application", "value": "DOGUS_ENERJI_UYGULAMA"}, {"key": "language", "value": "TR"}],
                "body": {"deliveryDay": f"{curr_d.strftime('%Y-%m-%d')}T00:00:00.000+0300"}
            }
            r2 = session.post(GOP_ENDPOINT, headers={"gop-service-ticket": st2, "Content-Type": "application/json"}, json=req_no_region)
            if r2.status_code == 200:
                try:
                    j2 = r2.json()
                    if isinstance(j2, dict) and str(j2.get("resultCode")) == "0":
                        rows2 = j2.get("body", {}).get("optimizationSummaryByOrganizations", [])
                        if rows2: all_rows.extend(rows2)
                except Exception:
                    pass
                    
        curr_d += timedelta(days=1)
        
    print(f"  [GOP Log] Toplam {len(all_rows)} satır bulundu.")
    if not all_rows: return pd.DataFrame(columns=["datetime", "damPurchasesVolume", "damPurchaseAmount", "damSalesVolume", "damSalesAmount"])

    df = pd.DataFrame(all_rows)
    df["period"] = pd.to_numeric(df["period"], errors="coerce").fillna(0).astype(int)
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    df["marketTradePrice"] = pd.to_numeric(df["marketTradePrice"], errors="coerce")
    
    df["deliveryDay"] = pd.to_datetime(df["deliveryDay"], errors="coerce")
    if getattr(df["deliveryDay"].dt, "tz", None) is not None:
        df["deliveryDay"] = df["deliveryDay"].dt.tz_localize(None)
    df["deliveryDay"] = df["deliveryDay"].dt.normalize()
    
    df["datetime"] = df["deliveryDay"] + pd.to_timedelta(df["period"] - 1, unit="h")
    
    alis = df[df["volume"] > 0].copy()
    alis["damPurchasesVolume"] = alis["volume"] / 10.0
    alis["damPurchaseAmount"] = alis["damPurchasesVolume"] * alis["marketTradePrice"]
    
    satis = df[df["volume"] < 0].copy()
    satis["damSalesVolume"] = satis["volume"].abs() / 10.0
    satis["damSalesAmount"] = satis["damSalesVolume"] * satis["marketTradePrice"]
    
    return pd.merge(alis[["datetime", "damPurchasesVolume", "damPurchaseAmount"]], satis[["datetime", "damSalesVolume", "damSalesAmount"]], on="datetime", how="outer").fillna(0.0)

def fetch_bpm_period(session, tgt, start_d, end_d):
    print(f"  [BPM Log] {start_d} / {end_d} arası çekiliyor...")
    all_items = []
    page = 1
    
    while True:
        st = get_st(session, tgt, "https://epys.epias.com.tr")
        
        payload = {
            "effectiveDateStart": f"{start_d.strftime('%Y-%m-%d')}T00:00:00+03:00",
            "effectiveDateEnd": f"{end_d.strftime('%Y-%m-%d')}T23:59:59+03:00",
            "page": {"number": page, "size": 1000},
            "region": "TR1"
        }
        r = session.post(BPM_URL, headers={"ST": st, "TGT": tgt, "Content-Type": "application/json"}, json=payload)
        
        if r.status_code != 200: 
            print(f"    [BPM Hata] HTTP {r.status_code} - Yanıt: {r.text[:200]}")
            break
        
        try:
            j = r.json()
            items = j.get("body", {}).get("content", {}).get("items", []) if isinstance(j, dict) else []
        except Exception:
            print(f"    [BPM Hata] JSON dönüştürülemedi. Yanıt: {r.text[:200]}")
            break
            
        if not items: break
        all_items.extend(items)
        if len(items) < 1000: break
        page += 1
        time.sleep(0.2)
        
    print(f"  [BPM Log] Toplam {len(all_items)} talimat bulundu.")
    if not all_items: return pd.DataFrame(columns=["datetime", "acceptedUpRegulation", "acceptedUpRegulationAmount", "acceptedDownRegulation", "acceptedDownRegulationAmount"])
    
    df = pd.json_normalize(all_items, sep="_")
    df["datetime"] = pd.to_datetime(df.get("effectiveDate")).dt.tz_localize(None).dt.floor("h")
    df["TalimatTipi"] = df.get("instructionType_label", "").astype(str).str.upper()
    df["energy"] = pd.to_numeric(df.get("energy", 0), errors="coerce").fillna(0)
    df["price"] = pd.to_numeric(df.get("price", 0), errors="coerce").fillna(0)
    df["amount"] = df["energy"] * df["price"]
    
    yal = df[df["TalimatTipi"] == "YAL"].groupby("datetime").agg(acceptedUpRegulation=("energy", "sum"), acceptedUpRegulationAmount=("amount", "sum")).reset_index()
    yat = df[df["TalimatTipi"] == "YAT"].groupby("datetime").agg(acceptedDownRegulation=("energy", "sum"), acceptedDownRegulationAmount=("amount", "sum")).reset_index()
    
    return pd.merge(yal, yat, on="datetime", how="outer").fillna(0.0)

def fetch_generation_period(session, tgt_seffaflik, start_d, end_d):
    print(f"  [Üretim Log] {start_d} / {end_d} arası çekiliyor... (Plant ID: {PLANT_ID})")
    payload = {
        "startDate": f"{start_d.strftime('%Y-%m-%d')}T00:00:00+03:00",
        "endDate": f"{end_d.strftime('%Y-%m-%d')}T23:59:59+03:00",
        "exportType": "CSV",
        "powerPlantId": PLANT_ID
    }
    r = session.post(REALTIME_URL, headers={"TGT": tgt_seffaflik, "Content-Type": "application/json"}, json=payload)
    print(f"    [Üretim Log] HTTP Status: {r.status_code}")
    if r.status_code != 200 or r.text.strip().startswith("{"): return pd.DataFrame(columns=["datetime", "generation"])
    
    try:
        df_csv = pd.read_csv(StringIO(r.text), sep=";", encoding="utf-8")
        if df_csv.empty: return pd.DataFrame(columns=["datetime", "generation"])
        
        df_csv.rename(columns=lambda x: x.strip() if isinstance(x, str) else x, inplace=True)
        date_col = df_csv.columns[0]
        hour_col = df_csv.columns[1]
        
        df_csv[date_col] = pd.to_datetime(df_csv[date_col], format="%d.%m.%Y", errors="coerce")
        df_csv["Hour"] = df_csv[hour_col].astype(str).str.extract(r'^(\d{2})')[0].astype(float).fillna(0)
        df_csv["datetime"] = df_csv[date_col] + pd.to_timedelta(df_csv["Hour"], unit="h")
        
        target_col = next((c for c in df_csv.columns if c.lower() == 'toplam'), None)
        if not target_col and len(df_csv.columns) > 2:
            target_col = df_csv.columns[2]
            
        print(f"    [Üretim Log] Değerler '{target_col}' sütunundan okunuyor.")
        print(f"    [Üretim Log - İlk 20 Satır Ham Veri]:\n{df_csv[[date_col, hour_col, target_col]].head(20).to_string(index=False)}")
        
        df_csv["generation"] = pd.to_numeric(df_csv[target_col].astype(str).str.replace(",", "."), errors="coerce").fillna(0.0)
        df_final = df_csv[["datetime", "generation"]].dropna().copy()
        
        print(f"    [Üretim Log] İşlem tamamlandı. {len(df_final)} saatlik üretim verisi alındı.")
        return df_final
    except Exception as e:
        print(f"    [Üretim Kritik Hata] CSV parse edilirken sorun oluştu: {e}")
        return pd.DataFrame(columns=["datetime", "generation"])

# ==========================
# ANA AKIŞ
# ==========================
def main():
    print("\n--- İŞLEM BAŞLIYOR ---")
    engine = get_db_engine()
    missing_days = get_missing_days(engine, START_DATE, END_DATE)
    
    if not missing_days:
        print("Tüm veriler güncel. Eksik gün bulunamadı.")
        return

    print(f"Toplam {len(missing_days)} eksik gün bulundu. Aylık olarak işleniyor...")

    months = groupby(missing_days, key=lambda d: (d.year, d.month))

    with requests.Session() as s:
        s.trust_env = False
        
        print("\n--- TOKEN ALMA İŞLEMLERİ ---")
        tgt_json = get_tgt(s, EPIAS_USER, EPIAS_PASS, "application/json", CAS_URL)
        tgt_text = get_tgt(s, EPIAS_USER, EPIAS_PASS, "text/plain", CAS_URL)
        tgt_seffaflik = get_tgt(s, SEFFAFLIK_USER, SEFFAFLIK_PASS, "text/plain", CAS_GIRIS_URL)
        print("Token alma işlemleri tamamlandı.\n")

        for (year, month), group in months:
            month_days = list(group)
            start_d = month_days[0]
            end_d = month_days[-1]
            
            print(f"\n=======================================================")
            print(f"--- {year}-{month:02d} Ayı Verileri Çekiliyor ({start_d} / {end_d}) ---")
            print(f"=======================================================")
            
            start_dt = datetime.combine(start_d, datetime.min.time())
            end_dt = datetime.combine(end_d, datetime.min.time()) + timedelta(hours=23)
            df_period = pd.DataFrame({"datetime": pd.date_range(start_dt, end_dt, freq="h")})
            
            df_gip = fetch_gip_period(s, tgt_json, start_d, end_d)
            df_gop = fetch_gop_period(s, tgt_text, start_d, end_d)
            df_bpm = fetch_bpm_period(s, tgt_text, start_d, end_d)
            df_gen = fetch_generation_period(s, tgt_seffaflik, start_d, end_d)
            
            df_period = df_period.merge(df_gop, on="datetime", how="left")
            df_period = df_period.merge(df_gip, on="datetime", how="left")
            df_period = df_period.merge(df_bpm, on="datetime", how="left")
            df_period = df_period.merge(df_gen, on="datetime", how="left")
            
            for col in FINAL_COLUMNS:
                if col not in df_period.columns:
                    df_period[col] = 0.5 if col == "consumption" else 0.0
            
            df_period = df_period[FINAL_COLUMNS].fillna(0.0).infer_objects(copy=False)
            df_period["consumption"] = df_period["consumption"].replace(0.0, 0.5)
            
            print(f"  [Veritabanı Log] Veritabanına {len(df_period)} saatlik satır yazılıyor...")
            delete_days_from_db(engine, start_d, end_d)
            df_period.to_sql(TABLE_NAME, engine, if_exists="append", index=False)
            print(f">>> {year}-{month:02d} ayı başarıyla kaydedildi.")

    print("\nTüm işlemler başarıyla tamamlandı.")

if __name__ == "__main__":
    main()