# MTA Subway Hourly Ridership

**Nama Kelompok:** Kelompok Smile  
**Anggota:**  
- Muhammad Abil Khoiri (101032330094)  
- Firdaus Arif Ramadhani (101032300131)  

**Topik Project:** Analisis dan Prediksi Volume Kepadatan Penumpang Kereta Bawah Tanah (*MTA Subway Hourly Ridership*) Menggunakan Integrasi Data *Real-Time* GTFS  

**Link Project:**  
- GitHub Repository: https://github.com/fxrdhan/mta-subway-bigdata  
- Sumber Data *Batch*: https://data.ny.gov/d/5wq4-mkjj  
- Sumber Data *Streaming*: https://api.mta.info/ (*MTA Real-Time GTFS Feed*)  

---

## I. Latar Belakang

Jaringan kereta bawah tanah (*subway*) *Metropolitan Transportation Authority* (MTA) New York City melayani jutaan komuter setiap hari melalui 428 kompleks stasiun. Tingginya volume mobilitas harian memicu risiko penumpukan penumpang (*overcrowding*) di peron dan ketidakefisienan alokasi frekuensi armada kereta. Selain itu, masa transisi sistem tiket dari kartu magnetik (*MetroCard*) ke pembayaran nirsentuh digital (*OMNY*) menuntut penataan gerbang masuk (*turnstile*) yang adaptif.

Melalui integrasi data historis volume penumpang per jam (*batch data*) dan pemantauan pergerakan armada secara langsung melalui *General Transit Feed Specification Realtime* (GTFS-RT *streaming*), sistem dapat memprediksi lonjakan komuter, mengkorelasikan keterlambatan kereta dengan kepadatan stasiun, serta mengoptimalkan operasional transit secara presisi.

---

## II. 5W1H

### What:
Analisis pola pergerakan, peramalan (*forecasting*) kepadatan penumpang per jam (*hourly ridership*), dan pemantauan keterlambatan armada kereta *real-time* pada 428 kompleks stasiun *subway* MTA New York City menggunakan *pipeline Big Data* dan *Machine Learning*.

### Why:
1. Memitigasi risiko penumpukan komuter berbahaya (*crowd safety*) di peron stasiun pada jam sibuk (*rush hour*).
2. Menyesuaikan frekuensi perjalanan kereta secara dinamis berbasis kebutuhan riil penumpang untuk efisiensi operasional dan energi.
3. Memetakan kecepatan adopsi tiket nirsentuh (*OMNY*) terhadap kartu fisik (*MetroCard*) guna optimalisasi fasilitas gerbang stasiun.

### Who:
1. *Metropolitan Transportation Authority* (MTA) dan *New York City Transit* (NYCT) sebagai operator utama pengambil kebijakan armada.
2. *New York City Department of Transportation* (NYC DOT) untuk integrasi antarmoda transportasi perkotaan.
3. Manajer Operasional dan Petugas Keamanan Stasiun untuk pengendalian kerumunan di lapangan.
4. Komuter dan masyarakat pengguna transportasi massal Kota New York.

### Where:
1. Data *Batch*: Portal Resmi Open Data Pemerintah Negara Bagian New York ([data.ny.gov](https://data.ny.gov/d/5wq4-mkjj)), diakses via SODA API ([data.ny.gov/resource/5wq4-mkjj.json](https://data.ny.gov/resource/5wq4-mkjj.json)).
2. Data *Streaming*: Portal Pengembang Resmi MTA ([api.mta.info](https://api.mta.info/)) berbasis spesifikasi protokol [GTFS Realtime](https://gtfs.org/realtime/).

### When:
1. Periode Data Historis (*Batch*): 1 Januari 2025 hingga September 2026 (terdiri dari 45.243.727 baris data terverifikasi tanpa *null*).
2. Pembaruan Data (*Streaming*): Data posisi armada (*Vehicle Positions*) dan estimasi kedatangan/keterlambatan (*Trip Updates*) yang mengalir setiap 30 detik secara kontinu.

### How:
1. *Pipeline & Ingestion*:
   - *Batch Processing*: Ekstraksi data via DuckDB dan Apache Arrow dengan format penyimpanan terkompresi Apache Parquet (zstd), mereduksi 45,2 juta baris menjadi 125 MB.
   - *Streaming Ingestion*: Penarikan *feed* GTFS-RT setiap 30 detik menggunakan Python Kafka Producer menuju *topic* Kafka (`mta-gtfs-stream`) untuk konsumsi *real-time* via Spark Streaming.
2. *Storage Management*:
   - SQL (PostgreSQL): Menyimpan data tabular agregat per stasiun, *Borough*, dan master rute perjalanan.
   - NoSQL (MongoDB): Menyimpan *payload* log peristiwa (*event*) GTFS-RT mentah dan metadata geospasial stasiun.
3. *Machine Learning*:
   - Regresi: Prediksi volume penumpang jam berikutnya ($\log(1 + \text{ridership})$) per stasiun.
   - Klasifikasi: Identifikasi kondisi jam sibuk (*Peak* vs *Off-Peak*) serta deteksi anomali keterlambatan jalur kereta.
   - *Clustering* (K-Means): Segmentasi 428 stasiun berdasarkan profil fluktuasi komuter (kawasan perkantoran, pemukiman, atau transit wisata).
