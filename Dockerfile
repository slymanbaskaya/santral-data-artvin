FROM python:3.11-slim

# Konteyner içindeki çalışma dizinini ayarlıyoruz
WORKDIR /app

# Sadece bu klasörün içindekileri konteynere kopyalıyoruz
COPY . .

# Gerekli kütüphaneleri kuruyoruz
RUN pip install --no-cache-dir -r requirements.txt

# Doğrudan master.py dosyasını çalıştırıyoruz
CMD ["python", "master.py"]