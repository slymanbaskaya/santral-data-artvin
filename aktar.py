import os
from dotenv import load_dotenv
load_dotenv()
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# 1. Veritabanı Bağlantı Bilgileri (Aiven)
GCP_HOST = os.getenv("GCP_HOST")
GCP_DB = os.getenv("GCP_DB")
GCP_USER = os.getenv("GCP_USER")
GCP_PASSWORD = os.getenv("GCP_PASSWORD")
GCP_PORT = os.getenv("GCP_PORT")

GCP_PASSWORD = quote_plus(GCP_PASSWORD)
engine = create_engine(f"postgresql+psycopg2://{GCP_USER}:{GCP_PASSWORD}@{GCP_HOST}:{GCP_PORT}/{GCP_DB}")

# 2. Verileri Veritabanından Okuma
print("Veritabanından tablolar okunuyor...")
df_ham = pd.read_sql("SELECT * FROM santral_data_artvin_ham", engine)

# Veritabanından ptf ve smf olarak çekiyoruz
df_ptf = pd.read_sql('SELECT datetime, ptf, smf, "positiveImbalance", "negativeImbalance" FROM epias_ptf_smf_sdf', engine)

# Kodun geri kalanıyla uyumlu olması için ptf -> mcp, smf -> smp olarak yeniden adlandırıyoruz
df_ptf.rename(columns={'ptf': 'mcp', 'smf': 'smp'}, inplace=True)

df_usd = pd.read_sql('SELECT datetime, usd_buying FROM tcmb_usd_kur_h', engine)

try:
    df_uzl = pd.read_sql("SELECT * FROM santral_data_artvin_uzl", engine)
except Exception as e:
    print("Uyarı: santral_data_artvin_uzl tablosu okunamadı. Boş kabul ediliyor.", e)
    df_uzl = pd.DataFrame(columns=df_ham.columns)

# 3. Saat Dilimlerini Temizleme ve Manuel Birleştirme
print("Veriler birleştiriliyor ve önceliklendiriliyor...")
df_ham['datetime'] = pd.to_datetime(df_ham['datetime']).dt.tz_localize(None)
df_ptf['datetime'] = pd.to_datetime(df_ptf['datetime']).dt.tz_localize(None)
df_usd['datetime'] = pd.to_datetime(df_usd['datetime']).dt.tz_localize(None)

df_main = df_ham.copy()
df_main['uzl'] = 0

if not df_uzl.empty:
    df_uzl['datetime'] = pd.to_datetime(df_uzl['datetime']).dt.tz_localize(None)
    
    # Mükerrer satırları engellemek için temizlik
    df_main.drop_duplicates(subset=['datetime'], inplace=True)
    df_uzl.drop_duplicates(subset=['datetime'], inplace=True)
    
    # İki tabloyu dış birleştirme (outer join) ile yan yana getir
    df_main = pd.merge(df_main, df_uzl, on='datetime', how='outer', suffixes=('', '_uzl'))
    
    # Sadece UZL'de gerçekten verisi olanları tespit et
    uzl_cols = ['positiveBalanceGroupImbalanceVolume', 'negativeBalanceGroupImbalanceVolume']
    valid_uzl_cols = [c + '_uzl' for c in uzl_cols if c + '_uzl' in df_main.columns]
    
    if valid_uzl_cols:
        df_main['uzl'] = df_main[valid_uzl_cols].notna().any(axis=1).astype(int)
    
    uzl_mask = df_main['uzl'] == 1
    
    # UZL'de bulunan TÜM sütunların HAM veriyi ezmesi (üzerine yazması) işlemi
    overlap_cols = [c for c in df_uzl.columns if c != 'datetime']
    for col in overlap_cols:
        if col in df_main.columns and col + '_uzl' in df_main.columns:
            df_main.loc[uzl_mask, col] = df_main.loc[uzl_mask, col + '_uzl']
            
    # İşimiz biten _uzl uzantılı geçici sütunları temizle
    cols_to_drop = [c for c in df_main.columns if c.endswith('_uzl')]
    df_main.drop(columns=cols_to_drop, inplace=True)
    df_main['uzl'] = df_main['uzl'].fillna(0).astype(int)

# Dengesizlik fiyatları (mcp ve smp dahil) birleşimi öncesi _x _y çakışma temizliği
overlap_cols_ptf = [c for c in df_ptf.columns if c in df_main.columns and c != 'datetime']
if overlap_cols_ptf:
    df_main.drop(columns=overlap_cols_ptf, inplace=True)

# Dengesizlik fiyatları ve kur verilerini ekle (SADECE BURADA BİR KERE YAPILMALI)
df_main = pd.merge(df_main, df_ptf, on='datetime', how='left')
df_main = pd.merge(df_main, df_usd, on='datetime', how='left')

# Mükerrer birleştirme sorunlarına karşı tarih sırasına sok
df_main.sort_values('datetime', inplace=True)
df_main.reset_index(drop=True, inplace=True)

# 4. Tüm Saatler İçin (UZL Dahil) Matematiksel Dengesizlik Hesaplaması
print("Tüm saatler için hacim ve tutar dengesizlik hesaplaması yapılıyor...")

calc_cols = ['generation', 'consumption', 'damSalesVolume', 'idmSalesVolume', 
             'acceptedUpRegulation', 'damPurchasesVolume', 'idmPurchasesVolume', 'acceptedDownRegulation']

for col in calc_cols:
    if col not in df_main.columns:
        df_main[col] = 0.0
    df_main[col] = pd.to_numeric(df_main[col], errors='coerce').fillna(0).astype('float64')

# UZL veya HAM fark etmeksizin tüm DataFrame için hesap formülünü uyguluyoruz
hesap = (df_main['generation'] - df_main['consumption']) - (
    df_main['damSalesVolume'] + 
    df_main['idmSalesVolume'] + 
    df_main['acceptedUpRegulation'] - 
    df_main['damPurchasesVolume'] - 
    df_main['idmPurchasesVolume'] - 
    df_main['acceptedDownRegulation']
)

pozitif_mask = (hesap > 0)
negatif_mask = (hesap < 0)

target_cols = [
    'positiveBalanceGroupImbalanceVolume', 'positiveBalanceGroupImbalanceAmount', 
    'negativeBalanceGroupImbalanceVolume', 'negativeBalanceGroupImbalanceAmount'
]

# Formülün üzerine temiz yazabilmesi için hedef sütunları sıfırlıyoruz (UZL'den gelen sıfırları eziyoruz)
for col in target_cols:
    df_main[col] = 0.0

df_main.loc[pozitif_mask, 'positiveBalanceGroupImbalanceVolume'] = hesap[pozitif_mask].astype('float64')
df_main.loc[pozitif_mask, 'positiveBalanceGroupImbalanceAmount'] = (hesap[pozitif_mask] * pd.to_numeric(df_main.loc[pozitif_mask, 'positiveImbalance'], errors='coerce')).astype('float64')

df_main.loc[negatif_mask, 'negativeBalanceGroupImbalanceVolume'] = hesap[negatif_mask].abs().astype('float64')
df_main.loc[negatif_mask, 'negativeBalanceGroupImbalanceAmount'] = (hesap[negatif_mask].abs() * pd.to_numeric(df_main.loc[negatif_mask, 'negativeImbalance'], errors='coerce')).astype('float64')


# 5. Düzeltilmiş Dengesizlik Maliyeti ve Gelir/Gider Hesaplamaları
print("Tüm saatler için nihai gelir/gider kalemleri oluşturuluyor...")

revenue_expense_cols = [
    'damSalesAmount', 'idmSalesAmount', 'acceptedUpRegulationAmount', 
    'damPurchaseAmount', 'idmPurchaseAmount', 'acceptedDownRegulationAmount'
]

for col in revenue_expense_cols:
    if col in df_main.columns:
        df_main[col] = pd.to_numeric(df_main[col], errors='coerce').fillna(0).astype('float64')
    else:
        df_main[col] = 0.0

# mcp (PTF) artık df_ptf üzerinden geldiği için sorunsuz çalışacak
mcp_num = pd.to_numeric(df_main.get('mcp', pd.Series(dtype='float64')), errors='coerce')

# Maliyet hesaplamaları
df_main['positiveImbalanceCost'] = ((df_main['positiveBalanceGroupImbalanceVolume'] * mcp_num) - df_main['positiveBalanceGroupImbalanceAmount']).fillna(0)
df_main['negativeImbalanceCost'] = (df_main['negativeBalanceGroupImbalanceAmount'] - (df_main['negativeBalanceGroupImbalanceVolume'] * mcp_num)).fillna(0)

# Toplam Dengesizlik Maliyeti
df_main['totalImbalanceCost'] = df_main['positiveImbalanceCost'] + df_main['negativeImbalanceCost']

df_main['totalRevenue'] = (
    df_main['damSalesAmount'] + 
    df_main['idmSalesAmount'] + 
    df_main['acceptedUpRegulationAmount'] + 
    df_main['positiveBalanceGroupImbalanceAmount'].fillna(0) 
)

df_main['totalExpense'] = (
    df_main['damPurchaseAmount'] + 
    df_main['idmPurchaseAmount'] + 
    df_main['acceptedDownRegulationAmount'] + 
    df_main['negativeBalanceGroupImbalanceAmount'].fillna(0)
)

df_main['netIncome'] = df_main['totalRevenue'] - df_main['totalExpense']

# 6. Veritabanına Aktarım
print("Nihai veriler veritabanına aktarılıyor...")
try:
    df_main.to_sql(name="santral_data_artvin", con=engine, if_exists='replace', index=False)
    print("İşlem başarıyla tamamlandı! Veriler tabloya eksiksiz işlendi.")
except Exception as e:
    print(f"Veritabanına aktarım hatası: {e}")
