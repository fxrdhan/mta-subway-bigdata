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
1. Periode Data Historis (*Batch*): 1 Januari 2025 hingga September 2026 (terdiri dari 45.244.036 baris data terverifikasi tanpa *null*).
2. Pembaruan Data (*Streaming*): Data posisi armada (*Vehicle Positions*) dan estimasi kedatangan/keterlambatan (*Trip Updates*) yang mengalir setiap 30 detik secara kontinu.

### How:
1. *Pipeline & Ingestion*:
   - *Batch Processing*: Ekstraksi data via DuckDB dan Apache Arrow dengan format penyimpanan terkompresi Apache Parquet (zstd), mereduksi 45,2 juta baris menjadi 124 MB.
   - *Streaming Ingestion*: Penarikan *feed* GTFS-RT setiap 30 detik menggunakan Python Kafka Producer menuju *topic* Kafka (`mta-gtfs-stream`) untuk konsumsi *real-time* via Spark Streaming.
2. *Storage Management*:
   - SQL (PostgreSQL): Menyimpan data tabular agregat per stasiun, *Borough*, dan master rute perjalanan.
   - NoSQL (MongoDB): Menyimpan *payload* log peristiwa (*event*) GTFS-RT mentah dan metadata geospasial stasiun.
3. *Machine Learning*:
   - Regresi: Prediksi volume penumpang jam berikutnya ($\log(1 + \text{ridership})$) per stasiun.
   - Klasifikasi: Identifikasi kondisi jam sibuk (*Peak* vs *Off-Peak*) serta deteksi anomali keterlambatan jalur kereta.
   - *Clustering* (K-Means): Segmentasi 428 stasiun berdasarkan profil fluktuasi komuter (kawasan perkantoran, pemukiman, atau transit wisata).

## Diagram Alir *Data Wrangling*

Diagram alir berikut merangkum tiga tahap *data wrangling* pada notebook: (a) *gathering* (bagian III), (b) *assessing*, dan (c) *cleaning* (bagian IV).

![Diagram alir data wrangling: (a) data gathering, (b) data assessing, (c) data cleaning](docs/diagrams/data_wrangling_flowchart.svg)

Sumber diagram dapat dibuka dan diedit di [Excalidraw](https://excalidraw.com/#json=vkhoVKhEYBX2iMafjqULz,B0VVTglBLE_UX4sAZNldlw) atau dari berkas [`docs/diagrams/data_wrangling_flowchart.excalidraw`](docs/diagrams/data_wrangling_flowchart.excalidraw).

## III. Data Wrangling – *Gathering Data*

Bagian ini mendokumentasikan tahap pengumpulan data (*gathering*) untuk dataset *MTA Subway Hourly Ridership*. Seluruh prosesnya dijalankan di Google Colab dan dijelaskan langkah demi langkah pada notebook proyek (bagian **III**).

- Notebook Google Colab: https://colab.research.google.com/drive/1GU64Yx-8Bq7PgxiG-lg5t9dnfb1lKIH5
- Notebook lengkap dengan output eksekusi: [`notebooks/MTA_Subway_Hourly_Ridership.ipynb`](notebooks/MTA_Subway_Hourly_Ridership.ipynb)
- Salinan kode notebook di repositori: [`notebooks/minggu3_gathering_data.ipynb`](notebooks/minggu3_gathering_data.ipynb)
- Laporan tugas Minggu 3: [`docs/TugasMinggu3_BIG-DATA_TK47G06_101032330094-101032300131.pdf`](docs/TugasMinggu3_BIG-DATA_TK47G06_101032330094-101032300131.pdf) ([DOCX](docs/TugasMinggu3_BIG-DATA_TK47G06_101032330094-101032300131.docx))
- Dataset final: [`mta_subway_hourly_ridership_2025-01_2026-09.parquet`](https://drive.google.com/file/d/1NqbE12P5M6n3l-Q3FYXfoErJgZijpI3k/view?usp=sharing) (124,2 MB, Google Drive). Berkas ini tidak di-*commit* ke repositori karena ukurannya melebihi batas file GitHub (100 MB).

### 1. Sumber dan Cara Memperoleh Data

| Item | Keterangan |
|---|---|
| Nama dataset | MTA Subway Hourly Ridership: Beginning 2025 |
| ID dataset | `5wq4-mkjj` |
| Pemilik | *Metropolitan Transportation Authority* (MTA) |
| Portal | [data.ny.gov/d/5wq4-mkjj](https://data.ny.gov/d/5wq4-mkjj) (NY Open Data) |
| Endpoint data | `https://data.ny.gov/resource/5wq4-mkjj` (format `.json` dan `.csv`, SODA API) |
| Endpoint metadata | `https://data.ny.gov/api/views/5wq4-mkjj.json` |
| Frekuensi pembaruan | Mingguan (menurut metadata portal) |
| Lisensi | Kolom lisensi pada metadata portal kosong, sehingga tidak dinyatakan eksplisit |

Data diperoleh langsung dari sumber utama lewat **SODA API** menggunakan permintaan HTTP GET (pustaka `requests`) tanpa autentikasi. Pengambilan lewat API dipilih dibanding unduhan manual karena data terus diperbarui dan API mendukung penyaringan (`$where`), pengurutan (`$order`), pembagian halaman (`$limit`, `$offset`), dan agregasi di sisi server (`$select`, `$group`). Fitur-fitur ini diperlukan untuk memfiksasi *snapshot* dan memvalidasi hasilnya.

Sumber lain yang dipertimbangkan:

- **MTA Subway Hourly Ridership: 2020-2024** (`wujg-7c2s`, 120.855.567 baris) memiliki skema identik dan menyambung tanpa celah dengan dataset utama (31 Desember 2024 pukul 23:00 lalu 1 Januari 2025 pukul 00:00). Dataset ini hanya diperiksa metadatanya dan tidak dimasukkan karena ruang lingkup proyek difiksasi pada data 2025 dan seterusnya.
- **Feed GTFS-RT MTA** (sumber *streaming*) tidak termasuk dalam dataset yang difiksasi pada tahap ini dan akan dikumpulkan pada tahap *streaming*.

### 2. Karakteristik Awal Dataset

| Karakteristik | Nilai |
|---|---|
| Granularitas | 1 baris = jam × kompleks stasiun × metode pembayaran × kelas tarif |
| Periode (*snapshot*) | 1 Januari 2025 00:00 s.d. 16 September 2026 23:00 (624 hari), kondisi server setelah pembaruan 30 September 2026 |
| Jumlah baris | 45.244.036 |
| Jumlah kolom | 12 |
| Kompleks stasiun | 428 |
| Moda transportasi | 3: *subway* (99,27% baris), *Staten Island Railway* (0,39%), *tram* (0,35%) |
| *Borough* | 5: Brooklyn (35,34%), Manhattan (30,90%), Queens (17,37%), Bronx (16,00%), Staten Island (0,39%) |
| Metode pembayaran | OMNY (55,41% baris; 87,7% total penumpang) dan MetroCard (44,59% baris) |
| Kelas tarif | 12 kategori |
| `ridership` | min 1; median 7; rata-rata 49,49; persentil ke-95 188; maks 23.510; total 2.238.989.512 |
| `transfers` | min −30; median 0; rata-rata 2,47; maks 2.556; total 111.767.871 |
| Nilai kosong (*null*) | 0 pada seluruh kolom |
| Ukuran | 374,3 MB (624 file harian) dan 124,2 MB (1 file final), format Parquet zstd |

| No | Kolom | Tipe (Arrow) | Keterangan |
|:-:|---|---|---|
| 1 | `transit_timestamp` | `timestamp[us]` | Waktu lokal New York, dibulatkan ke bawah ke jam terdekat, tanpa zona waktu |
| 2 | `transit_mode` | `string` | `subway`, `staten_island_railway`, atau `tram` |
| 3 | `station_complex_id` | `string` | Pengenal kompleks stasiun |
| 4 | `station_complex` | `string` | Nama kompleks stasiun beserta rute |
| 5 | `borough` | `string` | Wilayah administratif |
| 6 | `payment_method` | `string` | `omny` atau `metrocard` |
| 7 | `fare_class_category` | `string` | Kelas tarif |
| 8 | `ridership` | `float64` | Jumlah penumpang masuk |
| 9 | `transfers` | `float64` | Subset `ridership` yang masuk lewat transfer gratis |
| 10 | `latitude` | `float64` | Lintang stasiun |
| 11 | `longitude` | `float64` | Bujur stasiun |
| 12 | `georeference` | `string` | Titik lokasi format WKT `POINT (bujur lintang)` |

Catatan awal untuk tahap *assessing* dan *cleaning* (belum diperbaiki pada tahap ini): 226 baris `transfers` bernilai negatif, satu jam tidak muncul pada rentang waktu (2026-03-08 02:00, peralihan DST), satu kompleks stasiun (*St George*, ID 501) tercatat dengan dua moda, dan `transit_timestamp` tidak memuat zona waktu. Seluruh catatan ini ditangani pada bagian IV.

### 3. Tahapan Gathering

| No | Tahap | Metode | Bagian notebook | Hasil |
|:-:|---|---|:-:|---|
| 1 | Verifikasi sumber dan metadata | `GET /api/views/5wq4-mkjj.json` | 4.1 | Nama, pemilik, frekuensi pembaruan, tipe kolom |
| 2 | Melihat bentuk mentah respons | `GET .json` dan `.csv` dengan `$limit=3` | 4.2 | Angka dikirim sebagai teks dan `georeference` berupa objek GeoJSON pada JSON |
| 3 | Menetapkan skema target | Skema `pyarrow` eksplisit | 4.3 | 12 kolom bertipe tetap |
| 4 | Fiksasi *snapshot* | `count(*)`, total nilai, dan `max(transit_timestamp)` di server | 5.1 | *Cutoff* 2026-09-16 23:00; 45.244.036 baris |
| 5 | Rencana pengambilan | `GROUP BY` tanggal di server | 5.2 | Jumlah baris untuk 624 hari |
| 6 | Pengambilan per hari | CSV per halaman, 6 unduhan paralel, *retry* dengan *backoff* | 6 | 624 file Parquet harian |
| 7 | Penggabungan | *Concat* harian → bulanan → final (DuckDB) | 7 | 21 file bulanan dan 1 file final |
| 8 | Pemeriksaan struktur awal | SQL DuckDB dan `pandas` | 8 | Dimensi, tipe, *null*, sebaran, statistik |
| 9 | Validasi | Perbandingan agregat harian dengan server (`pd.merge`), duplikat, dan urutan | 9 | 624 hari sama dengan server; 0 duplikat kunci; 0 baris tidak urut |

Rincian penting pada tahap pengambilan:

- **Endpoint CSV dengan skema tetap.** CSV dibaca `pyarrow` dengan tipe kolom yang ditentukan di awal, sehingga seluruh potongan seragam.
- **Satu hari per pekerjaan.** Filter waktu membatasi setiap permintaan ke satu hari dan tidak melewati *cutoff*. Bila hasilnya lebih dari 500.000 baris, data diambil per halaman dengan urutan `:id` yang deterministik.
- **Validasi per hari.** Jumlah baris hasil unduhan harus sama dengan jumlah dari server. Bila berbeda, pengambilan diulang hingga tiga kali.
- **Aman dilanjutkan.** Hari yang filenya sudah ada dengan jumlah baris benar dilewati, sehingga sesi Colab yang terputus dapat dilanjutkan tanpa mengunduh ulang.
- **Revisi mingguan sumber.** *Cutoff* membatasi rentang waktu, tetapi tidak membekukan isi data: setiap pembaruan mingguan, MTA juga menambah dan merevisi baris beberapa bulan terakhir. Pembaruan 30 September 2026 menambah 309 baris dan memperbarui 6.419.272 baris sebelum *cutoff* (bukti `:updated_at` pada bagian 5.1 notebook). Karena itu nilai acuan mengikuti kondisi server setelah pembaruan tersebut, dan salinan yang tetap adalah dataset final di Google Drive.

### 4. Dataset dan File yang Digunakan

| Lapisan | Lokasi (pada Colab) | Isi | Ukuran |
|---|---|---|---|
| Sumber | `data.ny.gov/resource/5wq4-mkjj` | Data asli di server | – |
| Harian | `/content/mta_subway/01_daily/YYYY-MM/YYYY-MM-DD.parquet` | 624 file | 374,3 MB |
| Bulanan | `/content/mta_subway/02_monthly/year=YYYY/month=MM/part-0.parquet` | 21 file, terurut menurut kunci | ≈ 124,8 MB |
| **Final** | `/content/mta_subway/03_final/mta_subway_hourly_ridership_2025-01_2026-09.parquet` | 1 file, 45.244.036 baris × 12 kolom | 124,2 MB |
| Bersih (bagian IV) | `/content/mta_subway/04_clean/mta_subway_hourly_ridership_clean_2025-01_2026-09.parquet` | 1 file, 45.244.036 baris × 13 kolom | 124,3 MB |

Pada repositori:

| Berkas | Keterangan |
|---|---|
| [`notebooks/MTA_Subway_Hourly_Ridership.ipynb`](notebooks/MTA_Subway_Hourly_Ridership.ipynb) | Notebook lengkap proyek beserta seluruh output eksekusi di Google Colab |
| [`notebooks/minggu3_gathering_data.ipynb`](notebooks/minggu3_gathering_data.ipynb) | Salinan kode bagian III versi Minggu 3 (*gathering data*) |
| [`docs/TugasMinggu3_BIG-DATA_TK47G06_101032330094-101032300131.pdf`](docs/TugasMinggu3_BIG-DATA_TK47G06_101032330094-101032300131.pdf) | Berkas laporan resmi tugas Minggu 3 (tersedia pula format [DOCX](docs/TugasMinggu3_BIG-DATA_TK47G06_101032330094-101032300131.docx)) |
| [`docs/diagrams/`](docs/diagrams/) | Diagram alir *data wrangling* (sumber Excalidraw dan SVG) |
| [`src/download_snapshot.py`](src/download_snapshot.py), [`src/soda.py`](src/soda.py) | Skrip pengambilan yang sama untuk dijalankan di komputer lokal |
| `data/final/` | Lokasi salinan lokal dataset final (tidak di-*commit* karena besar) |

Notebook yang sama juga dijalankan di komputer lokal pada 6 Oktober 2026. Hasilnya sama dengan Colab dalam jumlah baris (45.244.036), total `ridership` (2.238.989.512), total `transfers` (111.767.871), dan seluruh hasil pemeriksaan bagian III dan IV. Pengambilan lokal sebelumnya (`src/download_snapshot.py`, 29 September 2026) menghasilkan 45.243.727 baris karena dilakukan sebelum pembaruan data MTA 30 September 2026.

### 5. Proses Penggabungan Data

Data berasal dari satu dataset sumber, tetapi diterima dalam 624 potongan harian. Penggabungannya berupa **penggabungan vertikal** (*concat*, setara `UNION ALL`) karena seluruh potongan memiliki kolom dan tipe yang sama. *Join* tidak diperlukan karena tidak ada tabel lain yang perlu disambungkan. Operasi `pd.merge` dipakai pada tahap validasi untuk membandingkan agregat harian lokal dengan server.

1. **Halaman → hari.** Bila hasil satu hari lebih dari satu halaman (500.000 baris), halaman-halaman digabung dengan `pa.concat_tables`. Pada data ini hari terbanyak hanya 93.017 baris sehingga setiap hari cukup satu halaman.
2. **Contoh `pd.concat`.** Tiga hari pertama digabung dengan `pd.concat` untuk memperlihatkan operasinya (74.914 + 77.324 + 78.251 = 230.489 baris; lihat notebook bagian 7.1).
3. **Harian → bulanan.** Seluruh file harian pada satu bulan dibaca dengan pola `*.parquet`, diurutkan menurut kunci (`transit_timestamp`, `station_complex_id`, `payment_method`, `fare_class_category`), dan ditulis sebagai satu file Parquet. Jumlah baris tiap bulan divalidasi terhadap server.
4. **Bulanan → final.** Ke-21 file bulanan disambung berurutan menjadi satu file. Pengurutan ulang tidak diperlukan karena tiap file sudah terurut dan rentang waktu antarbulan tidak tumpang tindih.

DuckDB dipakai untuk data penuh karena membaca Parquet secara kolumnar dan dapat memakai disk bila memori tidak cukup, sedangkan `pd.concat` pada 45 juta baris akan membebani RAM Colab.

Hasil penggabungan harian → bulanan. Jumlah baris setiap bulan sama dengan jumlah di server.

| Bulan | Hari | Baris | Ukuran (MB) |
|:-:|:-:|--:|--:|
| 2025-01 | 31 | 2.398.982 | 6,2 |
| 2025-02 | 28 | 2.273.357 | 5,7 |
| 2025-03 | 31 | 2.599.249 | 6,7 |
| 2025-04 | 30 | 2.646.409 | 6,5 |
| 2025-05 | 31 | 2.776.119 | 6,8 |
| 2025-06 | 30 | 2.673.221 | 6,7 |
| 2025-07 | 31 | 2.711.975 | 6,8 |
| 2025-08 | 31 | 2.681.448 | 6,8 |
| 2025-09 | 30 | 2.581.490 | 6,6 |
| 2025-10 | 31 | 2.578.816 | 6,7 |
| 2025-11 | 30 | 2.341.100 | 6,3 |
| 2025-12 | 31 | 2.209.615 | 6,3 |
| 2026-01 | 31 | 1.859.429 | 5,9 |
| 2026-02 | 28 | 1.658.281 | 5,2 |
| 2026-03 | 31 | 1.840.773 | 5,8 |
| 2026-04 | 30 | 1.725.407 | 5,4 |
| 2026-05 | 31 | 1.757.955 | 5,5 |
| 2026-06 | 30 | 1.683.124 | 5,3 |
| 2026-07 | 31 | 1.731.874 | 5,5 |
| 2026-08 | 31 | 1.706.174 | 5,4 |
| 2026-09 | 16 | 809.238 | 2,7 |
| **Total** | **624** | **45.244.036** | **≈ 124,8** |

### 6. Kendala dan Cara Mengatasinya

| No | Kendala | Cara mengatasi |
|:-:|---|---|
| 1 | Ukuran data besar (45,2 juta baris) sehingga tidak praktis diambil dalam satu permintaan dan berisiko membebani memori Colab | Data dibagi per hari (624 potongan), disimpan sebagai Parquet terkompresi, dan digabung dengan DuckDB yang memproses secara kolumnar dan dapat memakai disk |
| 2 | Respons JSON mengirim seluruh angka sebagai teks dan `georeference` sebagai objek GeoJSON | Memakai endpoint CSV dengan skema `pyarrow` eksplisit, sehingga tipe kolom seragam di seluruh potongan |
| 3 | Layanan publik dapat memutus atau menolak permintaan (*timeout*, kode 429/5xx). Pada uji lokal terjadi satu `ReadTimeout` | Fungsi `soda_get` melakukan *retry* dengan jeda eksponensial (hingga 8 kali). `ReadTimeout` tersebut diulang otomatis dan berhasil. Pada run final di Colab tidak diperlukan *retry* |
| 4 | Dataset diperbarui mingguan, dan setiap pembaruan juga merevisi baris beberapa bulan terakhir, sehingga dua pengambilan pada waktu berbeda dapat menghasilkan dataset berbeda walau *cutoff*-nya sama | Rentang data dibatasi dengan *cutoff* `transit_timestamp <= 2026-09-16 23:00`; nilai acuan (jumlah baris, total `ridership`, total `transfers`) dicocokkan dengan server; riwayat revisi ditampilkan lewat `:updated_at`; dan salinan yang tetap disimpan di Google Drive |
| 5 | Kueri agregat pada server lambat (satu `count(*)` pernah memakan sekitar 30 detik) | Kueri agregat dibatasi pada empat kueri; seluruh validasi harian memakai satu kueri `GROUP BY` |
| 6 | Sesi Colab bersifat sementara sehingga file dan progres hilang saat sesi berakhir | Pengambilan bersifat *idempotent* (hari yang sudah ada dilewati; terbukti pada eksekusi ulang yang melewati seluruh 624 hari). Tersedia opsi menyalin dataset final ke Google Drive |
| 7 | Kualitas data awal: 226 baris `transfers` negatif, satu jam hilang, dan satu stasiun dengan dua moda | Tidak diubah pada tahap *gathering*. Dicatat dan ditangani pada tahap *assessing* dan *cleaning* (bagian IV) |

### 7. Hasil Akhir Proses Gathering

Hasil pemeriksaan pada run final notebook (VM Colab baru, 6 Oktober 2026):

| Pemeriksaan | Bagian notebook | Hasil |
|---|:-:|---|
| Baris, total `ridership`, dan total `transfers` sampai *cutoff* sama dengan nilai acuan | 5.1 | Sama |
| Baris harian = baris server | 6.2 | 45.244.036 = 45.244.036 |
| Baris tiap bulan = baris server dan rentang antarbulan tidak tumpang tindih | 7.2 | 21 dari 21 bulan sesuai |
| Tidak ada nilai *null* dan string kosong | 8.3 | 0 pada seluruh kolom |
| Agregat per hari (baris, `ridership`, `transfers`) sama dengan server | 9.1 | 624 hari dibandingkan, 0 berbeda |
| Tidak ada duplikat kunci dan data terurut menurut kunci (file bulanan) | 9.2 | 0 duplikat, 0 baris tidak urut |
| Waktu tidak mundur pada file final | 9.2 | 0 |

| Atribut | Nilai |
|---|---|
| File | `mta_subway_hourly_ridership_2025-01_2026-09.parquet` |
| Format | Apache Parquet (kompresi zstd) |
| Ukuran | 124,2 MB |
| Baris × kolom | 45.244.036 × 12 |
| Rentang waktu | 2025-01-01 00:00 s.d. 2026-09-16 23:00 |
| Kompleks stasiun | 428 |
| Total `ridership` | 2.238.989.512 |
| Waktu pengambilan 624 hari | 11,3 menit pada run final (6 unduhan paralel); pengambilan penuh di Colab memakan 8,6 sampai 19,8 menit, bergantung beban server |

Dataset final ini menjadi masukan tahap *assessing* dan *cleaning* (bagian IV).

### 8. Bukti Pelaksanaan di Google Colab

Gambar berikut diambil dari eksekusi notebook Minggu 3 di Google Colab (30 September 2026), sebelum pembaruan data MTA pada hari yang sama. Karena itu angkanya mengikuti *snapshot* saat itu (45.243.727 baris) dan masih memuat ringkasan validasi lama (bagian 9.3). Angka terkini ada pada bagian-bagian di atas dan pada notebook.

![Gambar 1](docs/screenshots/01_metadata_dataset.jpg)
*Gambar 1. Metadata dataset dari endpoint `/api/views` (nama, pemilik, frekuensi pembaruan, lisensi kosong).*

![Gambar 2](docs/screenshots/02_respons_json_mentah.jpg)
*Gambar 2. Bentuk mentah respons JSON: seluruh angka bertipe teks dan `georeference` berupa objek GeoJSON.*

![Gambar 3](docs/screenshots/03_fiksasi_cutoff.jpg)
*Gambar 3. Fiksasi *snapshot*: baris di server sampai *cutoff* (45.243.727) sama dengan nilai acuan dan tidak ada baris setelah *cutoff*.*

![Gambar 4](docs/screenshots/04_rencana_per_bulan.jpg)
*Gambar 4. Rencana pengambilan: jumlah hari dan baris per bulan dari agregasi server.*

![Gambar 5](docs/screenshots/05_grafik_baris.jpg)
*Gambar 5. Jumlah baris per bulan dan per hari.*

![Gambar 6](docs/screenshots/06_unduh_paralel.jpg)
*Gambar 6. Pengambilan 624 hari secara paralel: 624 dari 624 hari berhasil dalam 10,6 menit, 0 gagal.*

![Gambar 7](docs/screenshots/07_ringkasan_unduhan.jpg)
*Gambar 7. Ringkasan pengambilan: total baris terkumpul sama dengan total menurut server.*

![Gambar 8](docs/screenshots/08_penggabungan_bulanan.jpg)
*Gambar 8. Penggabungan harian → bulanan (21 file): jumlah baris tiap bulan sama dengan server.*

![Gambar 9](docs/screenshots/09_file_final.jpg)
*Gambar 9. Penggabungan bulanan → satu file final.*

![Gambar 10](docs/screenshots/10_dimensi_tipe.jpg)
*Gambar 10. Dimensi dataset final (45.243.727 baris × 12 kolom) dan tipe data.*

![Gambar 11](docs/screenshots/11_contoh_baris_info.jpg)
*Gambar 11. Contoh lima baris pertama dan ringkasan tipe (`info`) pada sampel acak 100.000 baris.*

![Gambar 12](docs/screenshots/12_nilai_kosong.jpg)
*Gambar 12. Pemeriksaan nilai kosong dan string kosong: 0 pada seluruh kolom.*

![Gambar 13](docs/screenshots/13_temuan_awal.jpg)
*Gambar 13. Temuan awal untuk tahap berikutnya: 226 baris `transfers` negatif dan satu stasiun dengan dua moda.*

![Gambar 14](docs/screenshots/14_banding_server.jpg)
*Gambar 14. Perbandingan agregat harian dengan server: 624 hari dibandingkan, 0 hari berbeda.*

![Gambar 15](docs/screenshots/15_duplikat_urutan.jpg)
*Gambar 15. Pemeriksaan duplikat kunci dan urutan data: 0 duplikat, 0 baris tidak urut.*

![Gambar 16](docs/screenshots/16_validasi.jpg)
*Gambar 16. Ringkasan validasi: 10 dari 10 pemeriksaan lulus.*

![Gambar 17](docs/screenshots/17_dataset_akhir.jpg)
*Gambar 17. Ringkasan dataset akhir hasil *gathering*.*

## IV. Data Wrangling – *Assessing* dan *Cleaning Data*

Bagian ini menilai kualitas dataset hasil *gathering* dengan aturan yang terukur, menandai setiap baris yang melanggar (*error tagging*), membersihkannya, lalu membuktikan hasilnya dengan metrik kualitas sebelum dan sesudah *cleaning*. Seluruh proses dijalankan dengan DuckDB pada notebook proyek (bagian **IV**), tanpa memuat 45 juta baris ke memori.

### 1. Aturan Kualitas Data

Setiap aturan ditulis sebagai kondisi SQL yang bernilai benar bila sebuah baris melanggarnya. Tingkat `error` berarti nilainya salah dan harus diperbaiki; `warning` berarti nilainya bukan salah catat tetapi perlu ditandai.

| Tag | Dimensi | Tingkat | Aturan |
|---|---|:-:|---|
| `missing_value` | *Completeness* | `error` | Ada kolom yang `NULL` |
| `invalid_category` | *Validity* | `error` | `transit_mode`, `payment_method`, atau `borough` di luar nilai yang terdokumentasi |
| `nonpositive_ridership` | *Validity* | `error` | `ridership` nol atau negatif |
| `negative_transfers` | *Validity* | `error` | `transfers` negatif |
| `transfers_exceed_ridership` | *Validity* | `error` | `transfers` lebih besar dari `ridership` |
| `non_integer_count` | *Validity* | `error` | `ridership` atau `transfers` bukan bilangan bulat |
| `outside_nyc` | *Validity* | `error` | Koordinat di luar wilayah New York City |
| `not_on_hour` | *Validity* | `error` | `transit_timestamp` tidak tepat pada pergantian jam |
| `fare_class_payment_mismatch` | *Consistency* | `error` | Awalan `fare_class_category` berbeda dengan `payment_method` |
| `station_mode_mismatch` | *Consistency* | `error` | `transit_mode` berbeda dengan moda dominan stasiunnya |
| `georeference_mismatch` | *Consistency* | `error` | `georeference` berbeda dengan `latitude` dan `longitude` |
| `dst_transition_hour` | *Accuracy* | `warning` | Jam lokal yang tidak ada atau terjadi dua kali karena peralihan DST |

Keunikan baris memakai hasil pemeriksaan duplikat kunci pada bagian III (0 duplikat).

### 2. Hasil Penandaan dan Temuan

Dari 45.244.036 baris, 3.672 baris bertanda. Aturan yang tidak tercantum di bawah ini memiliki 0 pelanggaran.

| Tag | Tingkat | Baris |
|---|:-:|--:|
| `negative_transfers` | `error` | 226 |
| `station_mode_mismatch` | `error` | 2 |
| `dst_transition_hour` | `warning` | 3.444 |

- **`transfers` negatif.** Seluruhnya baris OMNY di empat kompleks stasiun dan bertanggal 17 Mei sampai 16 September 2026, yaitu rentang yang masih direvisi MTA setiap minggu.

  | Kompleks stasiun | Baris | Minimum | Jumlah nilai negatif |
  |---|--:|--:|--:|
  | Beach 67 St (A) | 91 | −2 | −96 |
  | 14 St/6 Av (1,2,3,L,F,M) | 64 | −30 | −119 |
  | Metropolitan Av/Lorimer St (G,L) | 42 | −2 | −43 |
  | Woodhaven Blvd (J,Z) | 29 | −2 | −30 |

- **Moda St George (ID 501).** 116.098 baris tercatat `staten_island_railway`, tetapi 2 baris (18 Juni dan 8 September 2026, masing-masing 1 penumpang) tercatat `subway`.
- **Jam peralihan DST.** `transit_timestamp` adalah waktu lokal tanpa zona waktu, sehingga label jamnya ambigu saat peralihan DST.

  | Jam lokal | Jenis | Baris | `ridership` | Rata-rata jam yang sama ±7 hari |
  |---|---|--:|--:|--:|
  | 2025-03-09 02:00 | tidak ada | 1.255 | 4.340 | 12.636 |
  | 2025-11-02 01:00 | terulang | 2.189 | 47.605 | 27.060 |
  | 2026-03-08 02:00 | tidak ada | 0 | 0 | 14.775 |

  Jam yang terulang berisi hampir dua kali penumpang normal karena dua jam tercatat dalam satu label. Jam yang tidak ada justru berisi data pada 2025 tetapi kosong pada 2026, sehingga pencatatan DST tidak konsisten antartahun.
- **Nilai ekstrem `ridership`.** Lima kombinasi jam dan stasiun tertinggi seluruhnya di Grand Central-42 St pukul 17.00 pada hari kerja (tertinggi 25.147 penumpang), sehingga wajar dan bukan kesalahan.

### 3. Tindakan *Cleaning*

Tidak ada baris yang dihapus, karena seluruh temuan dapat diperbaiki atau cukup ditandai.

| Temuan | Tindakan | Alasan |
|---|---|---|
| `station_mode_mismatch` | `transit_mode` diganti dengan moda dominan stasiunnya | Satu kompleks stasiun dilayani satu moda; nilai yang menyimpang adalah salah catat |
| `negative_transfers` | `transfers` negatif diubah menjadi 0 | `transfers` adalah jumlah orang sehingga tidak mungkin negatif; `ridership` pada baris yang sama tidak berubah |
| `ridership` dan `transfers` bertipe desimal | Diubah ke bilangan bulat (`INTEGER`) | Keduanya hitungan orang dan seluruh nilainya bulat |
| `dst_transition_hour` | Ditandai dengan kolom baru `is_dst_transition`; nilai tidak diubah | Penumpangnya nyata, tetapi label jamnya ambigu sehingga tidak dapat dipisah atau dipindah |
| Nilai ekstrem `ridership` | Dipertahankan | Terjadi pada jam sibuk sore di Grand Central-42 St |

Kueri *cleaning* hanya berupa proyeksi tanpa *join*, sehingga urutan baris file final tetap terjaga dan file bersih tidak perlu diurutkan ulang.

### 4. Validasi dan Metrik Kualitas

Seluruh aturan dijalankan ulang pada file bersih: tag `error` menjadi 0, dan 3.444 baris `warning` seluruhnya bertanda `is_dst_transition`. Skor setiap dimensi adalah persentase baris yang lolos seluruh aturan `error` pada dimensi tersebut.

| Dimensi | Gagal (sebelum) | Skor sebelum | Gagal (sesudah) | Skor sesudah |
|---|--:|--:|--:|--:|
| *Completeness* | 0 | 100,000000% | 0 | 100,000000% |
| *Consistency* | 2 | 99,999996% | 0 | 100,000000% |
| *Validity* | 226 | 99,999500% | 0 | 100,000000% |
| *Uniqueness* | 0 | 100,000000% | 0 | 100,000000% |
| Seluruh aturan baris | 228 | 99,999496% | 0 | 100,000000% |

| Ukuran | Sebelum | Sesudah | Selisih |
|---|--:|--:|--:|
| Baris | 45.244.036 | 45.244.036 | 0 |
| Total `ridership` | 2.238.989.512 | 2.238.989.512 | 0 |
| Total `transfers` | 111.767.871 | 111.768.159 | +288 |
| Baris dengan `transfers` negatif | 226 | 0 | −226 |

Waktu pada file bersih tidak pernah mundur (0 baris), sama dengan file final.

### 5. Dataset Bersih

| Atribut | Nilai |
|---|---|
| File | `mta_subway_hourly_ridership_clean_2025-01_2026-09.parquet` |
| Lokasi | `/content/mta_subway/04_clean/` di Colab, dengan salinan di Google Drive |
| Format | Apache Parquet (kompresi zstd) |
| Ukuran | 124,3 MB |
| Baris × kolom | 45.244.036 × 13 (12 kolom asli dan `is_dst_transition`) |
| Tipe yang berubah | `ridership` dan `transfers` menjadi `INTEGER`; `is_dst_transition` bertipe `BOOLEAN` |
| Baris bertanda `is_dst_transition` | 3.444 |
| Pelanggaran aturan `error` | 0 |
