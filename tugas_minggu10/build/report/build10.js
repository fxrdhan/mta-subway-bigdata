// Builds Dokumentasi_ETL (Tugas Minggu 10: implementasi ETL pipeline) in the same formal format as the Week 9 report.
// Usage: node build10.js <repo> <results10.json> <shots dir> <out.docx> [toc_pages.json]
// Figures come from the executed notebook (results10.json is extracted from it) and from screenshots in <shots dir>.
const fs = require("fs");
const path = require("path");
const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell, ImageRun, ExternalHyperlink,
  AlignmentType, HeadingLevel, WidthType, BorderStyle, ShadingType, LevelFormat, Footer, PageNumber,
  NumberFormat, TabStopType, LeaderType, Tab, SectionType, VerticalAlign, Bookmark, InternalHyperlink,
  PageOrientation, VerticalAlignSection,
} = require("docx");

const REPO = process.argv[2];
const R = JSON.parse(fs.readFileSync(process.argv[3], "utf8"));
const SHOTS = process.argv[4];
const OUT = process.argv[5];
const TOC_PAGES = process.argv[6] && fs.existsSync(process.argv[6]) ? JSON.parse(fs.readFileSync(process.argv[6], "utf8")) : {};
const DIAGRAM = path.join(REPO, "tugas_minggu10/diagram/implementasi_etl.png");
const LOGO = path.join(__dirname, "assets/logo_telkom.png");
const FONT = "Times New Roman";
const MONO = "Courier New";
const GRAY = "8C8C8C";

const CM = 567;
const PAGE = { width: 11906, height: 16838 };
const MARGIN = { top: 3 * CM, right: 3 * CM, bottom: 3 * CM, left: 4 * CM, header: 708, footer: 708 };
const CONTENT_W = PAGE.width - MARGIN.left - MARGIN.right;
const LINE = 360;

// Indonesian number formatting: 47.760.351 and 1,5.
const n = (x, d = 0) => Number(x).toLocaleString("id-ID", { minimumFractionDigits: d, maximumFractionDigits: d });
const idn = (v) => String(v).replace(/\d{1,3}(?:,\d{3})+/g, (m) => m.replace(/,/g, "."));
const idnRows = (rows) => rows.map((r) => r.map(idn));
const B = R.batch;
const S = R.stream;

function fit(widths) {
  const total = widths.reduce((a, b) => a + b, 0);
  const out = widths.map((w) => Math.round((w * CONTENT_W) / total));
  out[out.length - 1] += CONTENT_W - out.reduce((a, b) => a + b, 0);
  return out;
}

function runs(text, base = {}) {
  return String(text).split(/(\*\*[^*]+\*\*|\*[^*]+\*|`[^`]+`)/).filter(Boolean).map((part) => {
    if (part.startsWith("**")) return new TextRun({ ...base, text: part.slice(2, -2), bold: true });
    if (part.startsWith("`")) {
      const size = base.size ? base.size - 2 : 22;
      return new TextRun({ ...base, text: part.slice(1, -1), font: MONO, size });
    }
    if (part.startsWith("*") && part.length > 2) return new TextRun({ ...base, text: part.slice(1, -1), italics: true });
    return new TextRun({ ...base, text: part });
  });
}
const plain = (text) => String(text).replace(/[*`]/g, "");

const body = (text) => new Paragraph({
  children: runs(text), alignment: AlignmentType.JUSTIFIED,
  indent: { firstLine: 720 }, spacing: { after: 120, line: LINE },
});
let numberedList = 0;
const numbered = (items) => {
  numberedList += 1;
  return items.map((text) => new Paragraph({
    numbering: { reference: "numbers", level: 0, instance: numberedList }, children: runs(text),
    alignment: AlignmentType.JUSTIFIED, spacing: { after: 60, line: LINE },
  }));
};

const toc = [];
let chapter = 0;
let tableNo = 0;
let figureNo = 0;
function anchor(entry) {
  entry.anchor = `_Toc${String(toc.length + 1).padStart(3, "0")}`;
  toc.push(entry);
  return entry.anchor;
}
function bab(roman, title, { first = false } = {}) {
  chapter += 1; tableNo = 0; figureNo = 0;
  const id = anchor({ level: 0, text: `BAB ${roman} ${title}`, search: title });
  return new Paragraph({
    heading: HeadingLevel.HEADING_1, alignment: AlignmentType.CENTER, pageBreakBefore: !first,
    children: [new Bookmark({ id, children: [new TextRun({ text: `BAB ${roman}` }), new TextRun({ text: title, break: 1 })] })],
  });
}
function sub(num, title) {
  const text = `${num} ${title}`;
  const id = anchor({ level: 1, text, search: plain(text) });
  return new Paragraph({ heading: HeadingLevel.HEADING_2, children: [new Bookmark({ id, children: runs(text) })] });
}
function sub3(num, title) {
  const text = `${num} ${title}`;
  const id = anchor({ level: 2, text, search: plain(text) });
  return new Paragraph({ heading: HeadingLevel.HEADING_3, children: [new Bookmark({ id, children: runs(text) })] });
}
function unnumbered(title) {
  const id = anchor({ level: 0, text: title, search: title });
  return new Paragraph({ heading: HeadingLevel.HEADING_1, alignment: AlignmentType.CENTER, pageBreakBefore: true,
    children: [new Bookmark({ id, children: [new TextRun({ text: title })] })] });
}

const line = { style: BorderStyle.SINGLE, size: 4, color: GRAY };
const cellBorders = { top: line, bottom: line, left: line, right: line };
const none = { style: BorderStyle.NONE, size: 0, color: "FFFFFF" };
const noBorders = { top: none, bottom: none, left: none, right: none };

function cell(text, width, { header = false, keepNext = false, align } = {}) {
  return new TableCell({
    width: { size: width, type: WidthType.DXA }, borders: cellBorders,
    shading: header ? { type: ShadingType.CLEAR, color: "auto", fill: "E7E6E6" } : undefined,
    margins: { top: 50, bottom: 50, left: 90, right: 90 },
    children: [new Paragraph({
      alignment: header ? AlignmentType.CENTER : (align || AlignmentType.LEFT),
      spacing: { line: 240 }, keepNext,
      children: runs(text, { size: 19, bold: header || undefined }),
    })],
  });
}

function caption(kind, no, text, size) {
  return [new TextRun({ text: `${kind} ${chapter}.${no} `, bold: true, size }), ...runs(text, { size })];
}

const tableChecks = [];
// numeric: indexes of right-aligned columns
function table(title, rawWidths, header, rows, { numeric = [] } = {}) {
  tableNo += 1;
  const widths = fit(rawWidths);
  const trs = [new TableRow({ tableHeader: true, cantSplit: true,
    children: header.map((h, i) => cell(h, widths[i], { header: true, keepNext: true })) })];
  rows.forEach((row, r) => {
    const keepNext = r < rows.length - 1;
    trs.push(new TableRow({ cantSplit: true, children: row.map((c, i) => cell(c, widths[i],
      { keepNext, align: numeric.includes(i) ? AlignmentType.RIGHT : undefined })) }));
  });
  tableChecks.push({ caption: plain(`Tabel ${chapter}.${tableNo} ${title}`), last: rows[rows.length - 1].map(plain) });
  return [
    new Paragraph({ alignment: AlignmentType.CENTER, keepNext: true, spacing: { before: 200, after: 80 },
      children: caption("Tabel", tableNo, title, 22) }),
    new Table({ width: { size: CONTENT_W, type: WidthType.DXA }, columnWidths: widths, rows: trs }),
    new Paragraph({ spacing: { after: 120 }, children: [] }),
  ];
}
const tref = (k = 1) => `Tabel ${chapter}.${tableNo + k}`;
const fref = (k = 1) => `Gambar ${chapter}.${figureNo + k}`;

function pngSize(file) {
  const b = fs.readFileSync(file);
  return { w: b.readUInt32BE(16), h: b.readUInt32BE(20) };
}

// Images are scaled to the text width, and further down if they would be taller than maxHeightIn.
function figure(full, title, alt, { widthIn = 5.9, maxHeightIn = 8.2, before = 160 } = {}) {
  figureNo += 1;
  const { w, h } = pngSize(full);
  const wIn = Math.min(widthIn, (maxHeightIn * w) / h);
  const width = Math.round(wIn * 96);
  const height = Math.round((width * h) / w);
  return [
    new Paragraph({
      alignment: AlignmentType.CENTER, keepNext: true, spacing: { before, after: 60 },
      children: [new ImageRun({ type: "png", data: fs.readFileSync(full), transformation: { width, height },
        altText: { title: `Gambar ${chapter}.${figureNo}`, description: alt, name: path.basename(full) } })],
    }),
    new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 200 },
      children: caption("Gambar", figureNo, title, 22) }),
  ];
}
const shot = (file, title, alt, opts) => figure(path.join(SHOTS, file), title, alt, opts);
const landscapeFigure = (full, title, alt) => ({ landscape: figure(full, title, alt, { widthIn: 9.0, maxHeightIn: 5.6, before: 0 }) });

// ---------- Cover ----------
const center = (text, { size = 24, bold = false, after = 0, before = 0 } = {}) => new Paragraph({
  alignment: AlignmentType.CENTER, spacing: { before, after, line: 276 }, children: runs(text, { size, bold }),
});
function coverMembers() {
  const widths = [3800, 2000];
  const row = (name, nim) => new TableRow({ children: [name, nim].map((t, i) => new TableCell({
    width: { size: widths[i], type: WidthType.DXA }, borders: noBorders, verticalAlign: VerticalAlign.CENTER,
    margins: { top: 20, bottom: 20, left: 80, right: 80 },
    children: [new Paragraph({ alignment: i === 0 ? AlignmentType.LEFT : AlignmentType.RIGHT, children: runs(t) })],
  })) });
  return new Table({ alignment: AlignmentType.CENTER, width: { size: widths[0] + widths[1], type: WidthType.DXA },
    columnWidths: widths, borders: noBorders,
    rows: [row("Muhammad Abil Khoiri", "101032330094"), row("Firdaus Arif Ramadhani", "101032300131")] });
}
const logo = pngSize(LOGO);
const LOGO_W = Math.round(5 / 2.54 * 96);
const cover = [
  center("DOKUMENTASI IMPLEMENTASI", { size: 28, bold: true, after: 60 }),
  center("MATA KULIAH BIG DATA", { size: 24, bold: true, after: 400 }),
  center("IMPLEMENTASI *ETL PIPELINE*", { size: 32, bold: true, after: 60 }),
  center("UNTUK DATA PENUMPANG DAN KETERLAMBATAN", { size: 32, bold: true, after: 60 }),
  center("MTA SUBWAY", { size: 32, bold: true, after: 200 }),
  center("Tugas Minggu 10: Implementasi *ETL Pipeline*", { size: 24, after: 560 }),
  new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 560 }, children: [new ImageRun({
    type: "png", data: fs.readFileSync(LOGO),
    transformation: { width: LOGO_W, height: Math.round((LOGO_W * logo.h) / logo.w) },
    altText: { title: "Logo Universitas Telkom", description: "Logo vertikal Universitas Telkom", name: "logo_telkom.png" } })] }),
  center("Disusun oleh:", { after: 60 }),
  center("Kelompok Smile (Kelas TK47G06)", { bold: true, after: 120 }),
  coverMembers(),
  center("Dosen pengampu:", { before: 360, after: 60 }),
  center("Angel Metanosa Afinda, S.Kom., M.Kom. (MFC)", { after: 1000 }),
  center("PROGRAM STUDI S1 TEKNIK KOMPUTER", { size: 26, bold: true }),
  center("FAKULTAS TEKNIK ELEKTRO", { size: 26, bold: true }),
  center("UNIVERSITAS TELKOM", { size: 26, bold: true }),
  center("2026", { size: 26, bold: true }),
];

// ---------- Content ----------
const passed = (rows) => rows.filter((r) => r[4] === "Berhasil").length;
const content = [
  bab("I", "PENDAHULUAN", { first: true }),
  sub("1.1", "Studi Kasus dan Tujuan Pengolahan Data"),
  body("Proyek Kelompok Smile menganalisis dan memprediksi volume penumpang kereta bawah tanah New York berdasarkan *dataset* MTA Subway Hourly Ridership yang dipadukan dengan data *real-time* GTFS. Pada Minggu 9 telah disusun rancangan *pipeline* ETL yang terdiri atas jalur *batch* untuk data penumpang dan jalur *streaming* untuk data posisi serta prediksi kedatangan kereta (GTFS-Realtime). Dokumentasi ini memaparkan implementasi rancangan ETL tersebut menjadi *pipeline* yang benar-benar berjalan di klaster Kubernetes laboratorium, lengkap dengan hasil eksekusi dan pengujiannya."),
  body("Tujuan pengolahan data adalah menyediakan data bersih dan agregat di gudang data PostgreSQL yang siap dianalisis, baik pola penumpang maupun keterlambatan kereta. Dari jalur *batch* dihasilkan jumlah penumpang per jam, per kompleks stasiun per hari, dan per kategori tarif per hari, beserta tabel dimensi stasiun dan tanggal. Dari jalur *streaming* dihasilkan keterlambatan kereta per menit dan per jam serta catatan kedatangan kereta dan gangguan layanan. Sesuai pola ETL, seluruh transformasi dilakukan sebelum data dimuat dan data mentah tidak disimpan di *target storage*."),
  sub("1.2", "Sumber dan Karakteristik Dataset"),
  body(`Implementasi memproses tiga sumber data yang tercantum pada ${tref()}. Sumber utama adalah *dataset* MTA Subway Hourly Ridership yang diekstraksi seluruh riwayatnya, yaitu ${n(B.days)} hari dari ${B.first_day_id} sampai ${B.last_day_id} dengan ${n(B.rows)} baris. Atribut *dataset* tersebut tercantum pada ${tref(2)}.`),
  ...table("Sumber Data yang Diproses", [2100, 2500, 2400, 2026], ["Dataset", "Lokasi dan format", "Volume", "Pembaruan"], [
    ["MTA Subway Hourly Ridership: Beginning 2025", "data.ny.gov (SODA API), CSV", `${n(B.rows)} baris, ${n(B.days)} hari, 12 atribut`, "Mingguan; setiap pembaruan juga merevisi beberapa bulan terakhir"],
    ["MTA GTFS-Realtime Subway (8 *feed*)", "api-endpoint.mta.info, Protocol Buffers", `±16 pesan per menit, ±${n(S.mb_per_hour)} MB protobuf per jam sebelum kompresi`, "Sekitar 30 detik"],
    ["GTFS statis Subway", "rrgtfsfeeds (Amazon S3), ZIP berisi CSV", "18.406 perjalanan dan 556.613 jadwal berhenti", "Berkala; dimuat ulang setiap hari"],
  ]),
  ...table("Atribut Dataset MTA Subway Hourly Ridership", [2300, 1500, 5226], ["Atribut", "Tipe hasil ekstraksi", "Keterangan"], [
    ["`transit_timestamp`", "TIMESTAMP", "Awal jam pencatatan (waktu lokal New York)"],
    ["`transit_mode`", "VARCHAR", "Moda: *subway*, *staten_island_railway*, atau *tram*"],
    ["`station_complex_id`", "VARCHAR", "Kode kompleks stasiun"],
    ["`station_complex`", "VARCHAR", "Nama kompleks stasiun beserta jalurnya"],
    ["`borough`", "VARCHAR", "Wilayah: Bronx, Brooklyn, Manhattan, Queens, Staten Island"],
    ["`payment_method`", "VARCHAR", "Metode pembayaran: `omny` atau `metrocard`"],
    ["`fare_class_category`", "VARCHAR", "Kategori tarif, misalnya *OMNY - Full Fare*"],
    ["`ridership`", "DOUBLE", "Jumlah penumpang yang masuk dalam jam tersebut"],
    ["`transfers`", "DOUBLE", "Jumlah penumpang transfer dalam jam tersebut"],
    ["`latitude`, `longitude`", "DOUBLE", "Koordinat kompleks stasiun"],
    ["`georeference`", "VARCHAR", "Koordinat dalam format teks `POINT (lon lat)`"],
  ]),
  sub("1.3", "Target Storage"),
  body("*Target storage* mengikuti rancangan Minggu 9. Gudang data utama adalah PostgreSQL 16 pada basis data `mta`, yang untuk tugas ini memakai skema tersendiri bernama `etl` agar terpisah dari skema jalur ELT. Data dari jalur *streaming* yang berbentuk dokumen, yaitu kedatangan kereta dan gangguan layanan, disimpan di MongoDB 8 pada basis data `mta` dengan masa simpan 7 hari. Apache Kafka hanya berperan sebagai penyangga pesan dengan retensi 24 jam, bukan sebagai *target storage*."),
  sub("1.4", "Lingkungan dan Tools Implementasi"),
  body(`Seluruh komponen berjalan di *namespace* \`mta-pipeline\` pada klaster Kubernetes laboratorium, sedangkan PostgreSQL memakai layanan bersama yang sudah tersedia di klaster. Jalur *batch* ditulis dalam satu *notebook* Jupyter (\`ETL_Pipeline.ipynb\`) yang dieksekusi di klaster dengan \`jupyter nbconvert --execute\` sebagai Job Kubernetes, sehingga dekat dengan PostgreSQL. Jalur *streaming* berjalan terus-menerus sebagai tiga layanan. *Tools* yang dipakai tercantum pada ${tref()}, sedangkan alokasi sumber dayanya tercantum pada ${tref(2)}.`),
  ...table("Tools dan Library yang Dipakai", [2600, 1500, 4926], ["Tools atau library", "Versi", "Peran"], [
    ["Python", "3.13", "Bahasa pemrograman seluruh *pipeline*"],
    ["requests", "2.x", "Mengakses SODA API dan *feed* GTFS-RT"],
    ["PyArrow", "25.0.1", "Membaca CSV dengan skema tetap dan menulis Parquet di area *staging*"],
    ["DuckDB", "1.5.6", "Mesin transformasi *batch* di atas berkas Parquet"],
    ["pandas", "3.1.1", "Menampilkan hasil pemeriksaan dan pengujian di *notebook*"],
    ["psycopg", "3.x", "Koneksi ke PostgreSQL, perintah COPY, dan transaksi"],
    ["Jupyter nbconvert", "7.x", "Mengeksekusi *notebook* di klaster sebagai Job"],
    ["Apache Kafka (mode KRaft)", "4.3.1", "Penyangga pesan GTFS-RT, topik `mta-gtfs-stream`"],
    ["confluent-kafka", "2.16.1", "Klien Kafka pada *producer* dan *notebook*"],
    ["Apache Spark Structured Streaming", "4.2.1", "Transform dan Load jalur *streaming* per *micro-batch*"],
    ["nyct-gtfs", "2.x", "Definisi protobuf GTFS-Realtime dan ekstensi NYCT"],
    ["PyMongo dan MongoDB", "4.x dan 8.0.38", "Menyimpan kedatangan kereta dan gangguan layanan"],
    ["PostgreSQL", "16", "Gudang data, skema `etl`"],
  ]),
  ...table("Komponen di Kubernetes dan Alokasi Sumber Daya", [2900, 2000, 1900, 2226], ["Komponen", "Bentuk", "CPU (*request*)", "RAM (*request*/batas)"], [
    ["Job `etl-notebook` (jalur *batch*)", "Job, sekali jalan", "2 vCPU", "3 GiB / 4 GiB"],
    ["`mta-gtfs-producer`", "Deployment", "0,1 vCPU", "128 MiB / 256 MiB"],
    ["`mta-kafka`", "StatefulSet", "0,5 vCPU", "1 GiB / 1,5 GiB"],
    ["`mta-spark-streaming`", "Deployment", "1 vCPU", "2 GiB / 2,5 GiB"],
    ["`mta-mongo`", "StatefulSet", "0,25 vCPU", "768 MiB / 1 GiB"],
  ]),

  bab("II", "IMPLEMENTASI ETL PIPELINE"),
  body("Bab ini memaparkan implementasi setiap tahap ETL. Subbab 2.1 sampai 2.3 membahas jalur *batch*, subbab 2.4 membahas jalur *streaming*, subbab 2.5 membahas integrasi seluruh tahap, dan subbab 2.6 membahas pengujian. Seluruh angka pada bab ini berasal dari hasil eksekusi *notebook* `ETL_Pipeline.ipynb` di klaster pada " + R.run_date_id + "."),
  sub("2.1", "Implementasi Extract"),
  sub3("2.1.1", "Sumber Data dan Proses Ekstraksi"),
  body(`Data diambil dari SODA API data.ny.gov dengan HTTP GET tanpa autentikasi. Sebelum ekstraksi, server ditanya *fingerprint* setiap hari dalam satu kueri agregat, yaitu jumlah baris, total \`ridership\`, total \`transfers\`, dan waktu revisi terakhir (\`max(:updated_at)\`). Hasilnya ${n(B.server_days)} hari dengan ${n(B.server_rows)} baris. *Fingerprint* ini dibandingkan dengan *fingerprint* yang terakhir dimuat di tabel \`etl.ingest_day\`; hari yang berbeda dianggap berubah. Karena ETL tidak menyimpan data mentah, agregat satu bulan hanya bisa dibangun ulang dari seluruh hari di bulan tersebut sehingga yang diekstraksi adalah semua hari pada bulan yang berubah. Pada *run* pertama, tabel tersebut masih kosong sehingga seluruh ${n(B.days)} hari diekstraksi.`),
  body(`Ekstraksi berjalan paralel dengan ${B.workers} *thread*. Setiap hari diambil dalam halaman berisi paling banyak 500.000 baris yang diurutkan menurut \`:id\`, lalu CSV dibaca dengan skema tetap oleh PyArrow sehingga tipe setiap atribut sama di semua hari. Jumlah baris setiap hari dicocokkan dengan *fingerprint* server; bila berbeda, hari tersebut diambil ulang. Permintaan yang gagal karena HTTP 429, galat 5xx, atau gangguan jaringan diulang dengan jeda eksponensial. Hasil setiap hari disimpan sementara sebagai berkas Parquet terkompresi zstd di area *staging*, yaitu volume sementara di dalam *pod* yang dihapus setelah Load berhasil.`),
  body(`Ekstraksi ${n(B.days)} hari selesai dalam ${n(B.extract_min, 1)} menit dan menghasilkan ${n(B.rows)} baris dengan ukuran berkas *staging* ${n(B.staging_mb)} MB. Jumlah ini sama persis dengan *fingerprint* server. Keluaran tahap ini ditunjukkan pada ${fref()}.`),
  ...shot("01_extract.png", "Keluaran Tahap Extract di *Notebook*", "Log ekstraksi 674 hari dan waktu ekstraksi"),
  sub3("2.1.2", "Pemeriksaan Awal Data"),
  body(`Pemeriksaan awal dijalankan dengan DuckDB langsung di atas berkas Parquet sehingga ${n(B.rows)} baris tidak perlu dimuat seluruhnya ke memori. Pemeriksaan mencakup jumlah baris dan atribut, tipe data, contoh baris, nilai kosong, serta duplikat. Duplikat dihitung dua kali, yaitu duplikat persis pada seluruh atribut dan duplikat kunci pada kombinasi waktu, kompleks stasiun, metode pembayaran, dan kategori tarif. Ringkasan kondisi awal tercantum pada ${tref()}, sedangkan keluarannya ditunjukkan pada ${fref()}.`),
  ...table("Ringkasan Kondisi Awal Data Hasil Ekstraksi", [3800, 5226], ["Aspek", "Hasil"], [
    ["Jumlah baris dan atribut", `${n(B.rows)} baris, ${B.cols} atribut`],
    ["Rentang waktu", `${B.first_day_id} sampai ${B.last_day_id} (${n(B.days)} hari)`],
    ["Kompleks stasiun", `${n(B.stations)} kompleks`],
    ["Tipe `ridership` dan `transfers`", `${B.type_before} (desimal), padahal berupa hitungan orang`],
    ["Nilai kosong", `${n(B.missing)} pada seluruh atribut`],
    ["Duplikat persis", `${n(B.dup_rows)} baris`],
    ["Duplikat kunci", `${n(B.dup_keys)} baris`],
    ["Total `ridership`", n(B.total_ridership)],
    ["Total `transfers`", n(B.total_transfers)],
  ]),
  ...shot("02_initial_check.png", "Pemeriksaan Awal: Struktur, Contoh Baris, Nilai Kosong, dan Duplikat", "Keluaran DESCRIBE, contoh baris, dan hitungan nilai kosong serta duplikat"),

  sub("2.2", "Implementasi Transform"),
  sub3("2.2.1", "Inspeksi Data"),
  body(`Inspeksi memakai dua belas aturan kualitas dari bagian IV *notebook* proyek yang diperiksa sekaligus dalam satu kueri. Setiap aturan dilengkapi penjelasan dampaknya bila tidak ditangani. Hasilnya tercantum pada ${tref()} dan ${fref()}. Aturan yang tidak dapat diperbaiki secara otomatis ditetapkan sebagai aturan penghenti: bila salah satunya dilanggar, *pipeline* berhenti sebelum Load.`),
  ...table("Hasil Inspeksi Dua Belas Aturan Kualitas", [2700, 1300, 5026], ["Aturan", "Pelanggaran", "Dampak bila tidak ditangani"],
    B.inspection.map(([rule, v, impact]) => [`\`${rule}\``, n(v), impact]), { numeric: [1] }),
  ...shot("03_inspection.png", "Hasil Inspeksi Data di *Notebook*", "Tabel pelanggaran per aturan kualitas"),
  sub3("2.2.2", "Data Cleaning dan Data Transformation"),
  body(`Permasalahan yang ditemukan dan penanganannya tercantum pada ${tref()}. Teknik dipilih berdasarkan permasalahan yang benar-benar ada pada data. Aturan lain bernilai nol sehingga tidak memerlukan penanganan, tetapi tetap diperiksa pada setiap *run* untuk data baru.`),
  ...table("Permasalahan Data dan Penanganannya", [2300, 1600, 2600, 2526], ["Masalah", "Atribut", "Penanganan", "Alasan"], [
    [`Satu kompleks tercatat dengan dua moda (${n(B.counts.station_mode_mismatch)} baris)`, "`transit_mode`", "Diganti moda dominan kompleks (*data standardization*)", "Kompleks St George adalah stasiun Staten Island Railway"],
    [`\`transfers\` negatif (${n(B.counts.negative_transfers)} baris)`, "`transfers`", "Diubah menjadi 0 (*invalid value handling*)", "Hitungan orang tidak mungkin negatif; jumlah perubahannya dicatat"],
    ["Hitungan bertipe desimal", "`ridership`, `transfers`", "Diubah ke INTEGER (*data type conversion*)", "Tidak ada pecahan (aturan `non_integer_count` bernilai 0)"],
    [`Jam peralihan DST (${n(B.counts.dst_transition_hour)} baris)`, "`transit_timestamp`", "Tidak dihapus, ditandai `is_dst_transition`", "Datanya asli; menghapusnya menghilangkan penumpang"],
    ["Atribut berulang", "`georeference`", "Dibuang setelah divalidasi (*attribute removal*)", "Isinya sama dengan `latitude` dan `longitude`"],
  ]),
  body(`Pembersihan dan pembentukan fitur dijalankan sebagai satu kueri DuckDB yang menulis data bersih \`clean\` sebagai berkas Parquet di area *staging*. Cara ini dipilih karena ${n(B.rows)} baris tidak muat di memori *pod* (batas 4 GiB) bila ditampung sebagai tabel. Dari data bersih tersebut dibangun tiga tabel fakta dan dua tabel dimensi seperti pada ${tref()}.`),
  ...table("Transformasi yang Diterapkan", [2300, 3600, 3126], ["Teknik", "Hasil", "Tujuan"], [
    ["*Feature creation*", "`date`, `hour_of_day`, `day_of_week`, `is_weekend`, `is_dst_transition`", "Analisis pola jam dan hari serta fitur model"],
    ["*Data aggregation*", `Penumpang per jam × metode pembayaran (${n(B.sizes.t_hourly)} baris)`, "Grafik jam sibuk seluruh jaringan"],
    ["*Data aggregation*", `Penumpang per hari × kompleks stasiun, termasuk OMNY (${n(B.sizes.t_daily_station)} baris)`, "Analisis per stasiun dan dataset hasil transformasi"],
    ["*Data aggregation*", `Penumpang per hari × kategori tarif (${n(B.sizes.t_daily_fare_class)} baris)`, "Analisis kategori tarif"],
    ["*Attribute transformation*", `\`dim_station\` (${n(B.sizes.t_dim_station)} baris) dan \`dim_date\` (${n(B.sizes.t_dim_date)} baris)`, "Atribut stasiun dan tanggal tidak diulang di setiap baris fakta"],
  ]),
  sub3("2.2.3", "Validasi dan Perbandingan Sebelum–Sesudah"),
  body(`Hasil transformasi divalidasi dengan sepuluh pemeriksaan pada ${tref()}; seluruhnya lulus. Perbandingan kondisi data sebelum dan sesudah transformasi tercantum pada ${tref(2)} dan ${fref()}.`),
  ...table("Validasi Hasil Transformasi", [6500, 2526], ["Pemeriksaan", "Hasil"], B.validation.map(([k, v]) => [k, v ? "Lulus" : "Tidak lulus"])),
  ...table("Perbandingan Kondisi Data Sebelum dan Sesudah Transformasi", [2400, 3100, 3526], ["Aspek", "Sebelum", "Sesudah"],
    B.comparison.map(([k, a, b]) => [k, idn(a), idn(b)])),
  body(`Jumlah baris tidak berubah karena tidak ada baris yang dihapus: tidak ditemukan duplikat maupun baris tidak valid yang harus dibuang. Total \`transfers\` naik dari ${n(B.total_transfers)} menjadi ${n(B.total_transfers_after)} karena ${n(B.counts.negative_transfers)} nilai negatif diubah menjadi 0, sedangkan total \`ridership\` tetap sama persis.`),
  ...shot("04_comparison.png", "Validasi dan Perbandingan Sebelum–Sesudah Transformasi", "Tabel validasi dan tabel perbandingan sebelum sesudah"),

  sub("2.3", "Implementasi Load"),
  sub3("2.3.1", "Koneksi dan Struktur Penyimpanan"),
  body(`Program terhubung ke PostgreSQL dengan psycopg 3. Kredensial dibaca dari variabel lingkungan yang diisi Kubernetes dari Secret sehingga tidak ada kredensial yang tertulis di *notebook*. Koneksi dibuka dalam mode *autocommit*, dan setiap proses muat dibungkus transaksi eksplisit agar perubahan satu bulan tersimpan seluruhnya atau tidak sama sekali. Struktur tabel jalur *batch* di skema \`etl\` tercantum pada ${tref()}.`),
  ...table("Struktur Tabel Jalur Batch di Skema etl", [2900, 2700, 3426], ["Tabel", "Kunci utama", "Isi"], [
    ["`fact_ridership_hourly`", "`hour`, `payment_method`", "Penumpang dan transfer per jam, tanda DST"],
    ["`fact_ridership_daily_station`", "`day`, `station_complex_id`", "Penumpang, transfer, dan penumpang OMNY per hari per kompleks"],
    ["`fact_ridership_daily_fare_class`", "`day`, `fare_class_category`", "Penumpang per hari per kategori tarif"],
    ["`dim_station`", "`station_complex_id`", "Nama, wilayah, moda, dan koordinat kompleks"],
    ["`dim_date`", "`date`", "Tahun, bulan, hari, hari dalam minggu, akhir pekan"],
    ["`ingest_day`", "`day`", "*Fingerprint* hari yang sudah dimuat (untuk *run* inkremental)"],
    ["`quality_month`", "`month`, `rule`", "Jumlah pelanggaran per aturan per bulan"],
    ["`pipeline_run`", "`run_id`", "Log setiap *run*: waktu, status, tahap, jumlah hari dan baris"],
  ]),
  sub3("2.3.2", "Proses Loading"),
  body("Untuk tabel fakta dipilih *upsert* per bulan: baris bulan yang diekstraksi dihapus lalu dimuat ulang dengan perintah COPY dalam satu transaksi. *Append* tidak dipilih karena menjalankan ulang *pipeline* atau menerima revisi mingguan MTA akan menggandakan baris. *Replace* seluruh tabel juga tidak dipilih karena setiap pembaruan mingguan harus memuat ulang seluruh riwayat, padahal yang berubah hanya beberapa bulan terakhir. Dengan *upsert* per bulan, revisi MTA menggantikan nilai lama dan *pipeline* aman dijalankan berulang kali. Tabel dimensi dimuat dengan `INSERT ... ON CONFLICT DO UPDATE`, sedangkan *fingerprint* hari yang dimuat dan jumlah pelanggaran per aturan per bulan dicatat dalam transaksi yang sama."),
  body(`Proses Load memuat ${n(B.loaded_facts)} baris fakta untuk ${B.months_loaded} bulan dalam ${n(B.load_s)} detik. Setelah itu, data dibaca kembali dari PostgreSQL untuk memastikan tabel terisi dan dapat diakses, seperti pada ${fref()}.`),
  ...shot("05_load.png", "Proses Load dan Pembacaan Kembali dari PostgreSQL", "Jumlah baris yang dimuat dan contoh data hasil kueri PostgreSQL"),
  sub3("2.3.3", "Konsistensi Data Hasil Transformasi dan Target Storage"),
  body(`Konsistensi diperiksa dengan membandingkan jumlah baris dan total \`ridership\` setiap tabel fakta antara hasil transformasi di DuckDB dan data yang tersimpan di PostgreSQL. Hasilnya tercantum pada ${tref()}: ketiga tabel sama persis. Struktur atribut juga dibaca dari \`information_schema\` dan sesuai dengan rancangan. Hasil kueri langsung ke PostgreSQL di luar *notebook* ditunjukkan pada ${fref(2)}.`),
  ...table("Konsistensi Hasil Transformasi dan Data di PostgreSQL", [2700, 1300, 1300, 1600, 1600, 526],
    ["Tabel", "Baris transformasi", "Baris PostgreSQL", "Total *ridership* transformasi", "Total *ridership* PostgreSQL", "Sama"],
    B.consistency.map((r) => [`\`${r[0]}\``, n(r[1]), n(r[2]), n(r[3]), n(r[4]), r[5] ? "Ya" : "Tidak"]), { numeric: [1, 2, 3, 4] }),
  ...shot("06_consistency.png", "Pemeriksaan Konsistensi di *Notebook*", "Tabel konsistensi jumlah baris dan total ridership"),
  ...shot("10_postgres.png", "Data Tersimpan di Skema etl PostgreSQL", "Keluaran psql: daftar tabel dan contoh data"),
  body(`Selain dimuat ke PostgreSQL, data harian per kompleks stasiun hasil transformasi diekspor sebagai \`transformed_dataset.csv\` (${n(B.csv_rows)} baris, ${n(B.csv_mb, 1)} MB) yang dilampirkan pada folder *Dataset*.`),

  sub("2.4", "Implementasi Jalur Streaming"),
  sub3("2.4.1", "Extract: Producer dan Apache Kafka"),
  body("*Producer* Python (`mta-gtfs-producer`) mengambil delapan *feed* GTFS-Realtime MTA secara paralel setiap 30 detik dan mengirim setiap *feed* sebagai satu pesan ke topik Kafka `mta-gtfs-stream`. *Key* pesan berisi nama *feed*, *value* berisi *bytes* protobuf, dan *header* `fetched_at` berisi waktu pengambilan; pesan dikompresi dengan zstd. Kafka berjalan sebagai satu *broker* mode KRaft dengan retensi 24 jam sehingga gangguan Spark selama beberapa jam tidak menghilangkan data. " +
    `Saat pemeriksaan, topik menyimpan ${n(S.kafka_stored)} pesan dan Spark telah memproses ${n(S.batches)} *micro-batch* ${S.lag === 0 ? "tanpa ketertinggalan (lag 0 pesan)" : `dengan lag ${n(S.lag)} pesan, tidak lebih dari satu putaran *producer* (8 pesan) yang menunggu *micro-batch* berikutnya`}, seperti pada ${fref()}.`),
  ...shot("11_kafka.png", "Topik Kafka dan Log Producer", "Deskripsi topik, offset, dan log producer"),
  body(`Pemeriksaan awal dilakukan pada jendela ${n(S.window_msgs)} pesan terakhir yang sudah diproses Spark (${S.window_range}). Jendela tersebut berisi ${n(S.entities)} entitas, yaitu ${n(S.trip_updates)} \`trip_update\`, ${n(S.vehicles)} \`vehicle\`, dan ${n(S.alerts_raw)} \`alert\`, dengan ${n(S.stop_time_updates)} prediksi kedatangan. Ditemukan ${n(S.arah_kosong)} perjalanan tanpa arah pada ekstensi NYCT, ${n(S.status_kosong)} kendaraan tanpa status, ${n(S.prediksi_lampau)} prediksi yang sudah lewat, dan ${n(S.snapshot_berulang)} *snapshot* berulang. *Feed* juga tidak memiliki atribut keterlambatan.`),
  sub3("2.4.2", "Transform: Spark Structured Streaming"),
  body(`Spark (\`mta-spark-streaming\`) membaca topik Kafka per *micro-batch* 30 detik dan menjalankan fungsi \`decode()\` serta \`aggregate()\` pada setiap pesan. Permasalahan yang ditangani tercantum pada ${tref()}. Keterlambatan dihitung sebagai selisih prediksi kedatangan di stasiun berikutnya dengan jadwal statis GTFS. Perjalanan *real-time* dicocokkan dengan jadwal statis melalui kunci waktu asal, rute, dan arah pada \`trip_id\`, misalnya \`044450_A..N\`, dengan memperhitungkan layanan yang berlaku pada tanggal perjalanan.`),
  ...table("Permasalahan Data GTFS-Realtime dan Penanganannya", [3000, 3300, 2726], ["Masalah", "Penanganan", "Alasan"], [
    ["*Snapshot* berulang karena *feed* belum diperbarui MTA", "Dilewati (*duplicate removal*)", "Kereta yang sama tidak terhitung dua kali"],
    ["Arah kosong pada ekstensi NYCT", "Diambil dari `trip_id` (`..S`), selain itu `N`", "Agregat dipisah per arah"],
    ["Status kendaraan kosong", "Diisi `IN_TRANSIT_TO`, nilai bawaan GTFS-RT", "Penghitungan kereta yang sedang berhenti"],
    ["Prediksi yang sudah lewat", "Dibuang; dipakai prediksi stasiun berikutnya", "Prediksi lampau bukan informasi baru"],
    ["Tidak ada atribut keterlambatan", "Dihitung dari prediksi dan jadwal statis (*feature creation*)", "Metrik utama analisis keterlambatan"],
    ["Keterlambatan lebih dari 3 jam", "Dikosongkan (*outlier*)", "Bukan perjalanan yang sama"],
    ["Perjalanan tidak ada di jadwal statis", "Keterlambatan kosong, observasi tetap dihitung", "Tidak ada jadwal pembanding"],
    ["`stop_id` berakhiran arah, misalnya `127N`", "Dipotong menjadi kode stasiun `127`", "Agregat per stasiun"],
  ]),
  body(`Agar dapat dibuktikan, *notebook* membaca pesan yang sama dari Kafka dan menjalankan fungsi \`decode()\` dan \`aggregate()\` dari modul yang sama dengan yang dijalankan Spark. Hasilnya tercantum pada ${tref()}: ${n(S.trip_updates)} \`trip_update\` menjadi ${n(S.obs)} observasi, dan ${S.matched_pct}% observasi memiliki nilai keterlambatan karena perjalanannya cocok dengan jadwal statis. Ketujuh pemeriksaan validasi lulus, seperti pada ${fref()}.`),
  ...table("Perbandingan Data GTFS-Realtime Sebelum dan Sesudah Transformasi", [2400, 3100, 3526], ["Aspek", "Sebelum (pesan mentah)", "Sesudah (hasil transformasi)"],
    S.comparison.map(([k, a, b]) => [k, idn(a), idn(b)])),
  ...shot("12_stream_transform.png", "Transformasi Data Streaming di *Notebook*", "Validasi dan perbandingan sebelum sesudah transformasi GTFS-RT"),
  sub3("2.4.3", "Load: PostgreSQL dan MongoDB"),
  body(`Hasil transformasi dimuat ke target pada ${tref()}. Tabel keterlambatan memakai *upsert* aditif karena data satu menit dapat tersebar di dua *micro-batch*. Agar penjumlahan tidak terulang ketika Spark memutar ulang *micro-batch* setelah *restart*, baris log \`batch_id\` ditulis dalam transaksi yang sama, dan *micro-batch* yang sudah tercatat dilewati (*exactly-once*). MongoDB ditulis lebih dahulu dengan *upsert* per \`_id\` sehingga penulisan ulang menghasilkan dokumen yang sama. MongoDB dipakai untuk kedatangan dan gangguan karena datanya berbentuk dokumen dengan atribut yang bervariasi dan hanya dibutuhkan sementara.`),
  ...table("Target Penyimpanan Jalur Streaming", [3000, 3600, 2426], ["Target", "Isi", "Strategi"], [
    ["PostgreSQL `etl.gtfs_delay_minute`", "Observasi, keterlambatan, dan kereta berhenti per menit × rute × arah", "*Upsert* aditif; retensi 14 hari"],
    ["PostgreSQL `etl.gtfs_delay_hourly`", "Keterlambatan per jam × stasiun × rute", "*Upsert* aditif"],
    ["PostgreSQL `etl.gtfs_stream_batch`", "Log setiap *micro-batch*: *offset* Kafka, jumlah baris, durasi", "*Insert*; kunci `batch_id`"],
    ["MongoDB `mta.arrivals`", "Kedatangan kereta di stasiun", "*Upsert* per `_id`; TTL 7 hari"],
    ["MongoDB `mta.alerts`", "Gangguan layanan yang aktif", "*Upsert* per `_id`; TTL 7 hari"],
  ]),
  body(`Saat pemeriksaan, \`etl.gtfs_delay_minute\` berisi ${n(S.pg_minute)} baris, \`etl.gtfs_delay_hourly\` ${n(S.pg_hourly)} baris, \`mta.arrivals\` ${n(S.mongo_arrivals)} dokumen, dan \`mta.alerts\` ${n(S.mongo_alerts)} dokumen. Konsistensi diperiksa dengan membandingkan agregat hasil *notebook* dan data yang dimuat Spark untuk ${S.full_minutes} menit lengkap di jendela pemeriksaan: ${n(S.keys_compared)} kunci menit × rute × arah dibandingkan dan ${n(S.keys_diff)} berbeda, serta ${n(S.arrivals_found)} dari ${n(S.arrival_ids)} kedatangan ditemukan di MongoDB (${fref()} dan ${fref(2)}).`),
  ...shot("13_stream_consistency.png", "Konsistensi Hasil Transformasi Streaming dan Data di PostgreSQL", "Tabel konsistensi per menit"),
  ...shot("14_mongodb.png", "Data Kedatangan dan Gangguan Layanan di MongoDB", "Keluaran mongosh: jumlah dokumen, indeks TTL, dan contoh dokumen"),

  sub("2.5", "Integrasi ETL Pipeline"),
  sub3("2.5.1", "Alur Integrasi dan Logging"),
  body("Pada jalur *batch*, seluruh tahap digabung dalam fungsi `run_etl()`. Keluaran setiap tahap menjadi masukan tahap berikutnya: *fingerprint* menentukan hari yang diekstraksi, berkas Parquet *staging* menjadi masukan DuckDB, dan tabel hasil transformasi menjadi masukan COPY ke PostgreSQL. Transform hanya berjalan bila Extract berhasil dan jumlah barisnya sama dengan *fingerprint*, sedangkan Load hanya berjalan bila validasi lulus dan jumlah baris tidak berubah selama transformasi. Setiap tahap mencatat pesan log, misalnya \"Data berhasil diekstraksi\", \"Data berhasil dibersihkan dan ditransformasi\", dan \"Data berhasil disimpan ke target storage\". Setiap *run* juga dicatat di tabel `etl.pipeline_run` beserta tahap tempat kegagalan terjadi, dan area *staging* dihapus setelah Load berhasil."),
  body(`*Run* pertama dijalankan bertahap di *notebook* agar hasil setiap tahap dapat diperiksa, lalu *pipeline* dijalankan kembali dari awal sampai akhir dengan \`run_etl()\`. ${B.second_text} Log kedua *run* tercantum pada ${tref()} dan ${fref()}.`),
  ...table("Log Eksekusi Pipeline Batch (etl.pipeline_run)", [2200, 1100, 900, 1300, 1600, 1926], ["Mulai (UTC)", "Status", "Hari", "Baris", "Bulan dimuat", "Keterangan"],
    B.runs.map((r) => [r[0], r[2], n(r[4]), n(r[5]), r[6] && r[6] !== "-" ? r[6] : "–", r[7] === "run_etl()" ? "*run* ulang dengan `run_etl()`" : r[7]]), { numeric: [2, 3] }),
  ...shot("07_pipeline_run.png", "Eksekusi Pipeline Batch secara Keseluruhan", "Keluaran run_etl dan tabel log pipeline_run"),
  body(`Pada jalur *streaming*, ketiga layanan berjalan sebagai Deployment dan StatefulSet Kubernetes yang dimulai ulang secara otomatis bila gagal. Spark menyimpan *checkpoint offset* di volume persisten sehingga setelah *restart* pemrosesan berlanjut dari *offset* terakhir, dan setiap *micro-batch* dicatat di \`etl.gtfs_stream_batch\`. Saat pemeriksaan, ${n(S.batches)} *micro-batch* tercatat dengan *offset* berurutan tanpa celah, rata-rata durasi ${n(S.avg_batch_s, 2)} detik, dan median latensi dari waktu *feed* sampai data tersimpan di PostgreSQL ${n(S.latency_s)} detik. Log Spark ditunjukkan pada ${fref()}, sedangkan seluruh komponen di Kubernetes ditunjukkan pada ${fref(2)}.`),
  ...shot("15_spark_logs.png", "Log *Micro-batch* Spark dan Hasilnya di PostgreSQL", "Log batch Spark, log gtfs_stream_batch, dan keterlambatan per menit"),
  ...shot("09_kubernetes_komponen.png", "Komponen ETL Pipeline di Kubernetes", "kubectl get job, deployment, statefulset, dan pod"),
  sub3("2.5.2", "Diagram Implementasi"),
  body(`Diagram implementasi pada ${fref()} menggambarkan komponen yang benar-benar dijalankan: Job \`etl-notebook\` untuk jalur *batch*, tiga layanan untuk jalur *streaming*, serta PostgreSQL, MongoDB, dan berkas CSV sebagai keluaran. Berbeda dengan diagram arsitektur Minggu 9, diagram ini memuat nama komponen Kubernetes, *library* yang dipakai di setiap tahap, dan mekanisme penyimpanan yang diterapkan.`),
  landscapeFigure(DIAGRAM, "Diagram Implementasi ETL Pipeline", "Diagram implementasi jalur batch dan streaming di klaster laboratorium"),
  sub3("2.5.3", "Hasil Eksekusi Keseluruhan"),
  body(`Eksekusi *notebook* di klaster memakan waktu total ${n(R.total_min)} menit, dengan ${n(B.extract_min, 1)} menit di antaranya untuk Extract yang dibatasi kecepatan SODA API. Seluruh sel berjalan tanpa galat, ${n(B.rows)} baris mentah diproses menjadi ${n(B.loaded_facts)} baris fakta, dan area *staging* dihapus setelah Load. Jalur *streaming* berjalan terus-menerus selama eksekusi tersebut dan tetap berjalan setelahnya.`),

  sub("2.6", "Testing dan Validation"),
  body(`Pengujian dijalankan di bagian akhir setiap jalur pada *notebook*. Hasil pengujian jalur *batch* tercantum pada ${tref()}, sedangkan jalur *streaming* pada ${tref(2)}. Seluruh ${B.tests.length + S.tests.length} pengujian berhasil.`),
  ...table("Hasil Pengujian Jalur Batch", [1300, 1900, 2200, 2700, 926], ["Tahapan", "Aspek yang diuji", "Hasil yang diharapkan", "Hasil aktual", "Status"], idnRows(B.tests)),
  ...table("Hasil Pengujian Jalur Streaming", [1300, 1900, 2200, 2700, 926], ["Tahapan", "Aspek yang diuji", "Hasil yang diharapkan", "Hasil aktual", "Status"], idnRows(S.tests)),
  body(`Jumlah data pada setiap tahap tercantum pada ${tref()} untuk jalur *batch* dan ${tref(2)} untuk jalur *streaming*.`),
  ...table("Jumlah Data pada Setiap Tahap Jalur Batch", [2600, 2000, 4426], ["Tahapan", "Jumlah baris", "Keterangan"], idnRows(B.row_flow), { numeric: [1] }),
  body("Jumlah baris setelah Extract dan setelah Transform sama karena tidak ada baris yang dihapus. Jumlah baris setelah Load lebih kecil karena yang dimuat ke PostgreSQL adalah agregat, sesuai rancangan ETL Minggu 9 yang hanya menyimpan data bersih teragregasi di gudang data. Tidak ada data yang hilang: total `ridership` sama persis antara data mentah, data bersih, tabel fakta, dan *fingerprint* server untuk setiap hari."),
  ...table("Jumlah Data pada Setiap Tahap Jalur Streaming (Menit Lengkap)", [2600, 2400, 4026], ["Tahapan", "Jumlah", "Keterangan"], idnRows(S.row_flow)),
  body("Pada jalur *streaming*, satu pesan berisi satu *snapshot* *feed* dengan ratusan entitas. Setiap perjalanan yang masih berjalan menghasilkan satu observasi pada stasiun berikutnya, lalu observasi diagregasi per menit × rute × arah. Jumlah baris agregat hasil *notebook* sama dengan jumlah baris yang dimuat Spark ke PostgreSQL."),
  ...shot("08_tests.png", "Hasil Pengujian dan Jumlah Data Jalur Batch di *Notebook*", "Tabel pengujian dan jumlah baris per tahap"),
  ...shot("16_stream_tests.png", "Hasil Pengujian Jalur Streaming di *Notebook*", "Tabel pengujian streaming dan ringkasan total"),

  bab("III", "ANALISIS DAN KESIMPULAN"),
  sub("3.1", "Evaluasi Keberhasilan Implementasi"),
  body(`Implementasi ETL dinilai berhasil. Pada jalur *batch*, seluruh riwayat ${n(B.days)} hari (${n(B.rows)} baris) berhasil diekstraksi dengan jumlah baris yang sama persis dengan *fingerprint* server, dibersihkan tanpa kehilangan data, dan dimuat ke skema \`etl\` PostgreSQL. Data yang dibaca kembali sama dengan hasil transformasi, baik jumlah baris maupun total \`ridership\`, dan total harian di gudang data sama dengan *fingerprint* server untuk semua hari. *Run* kedua menunjukkan bahwa *pipeline* aman dijalankan ulang tanpa menggandakan data. Pada jalur *streaming*, data GTFS-Realtime mengalir dari *producer* melalui Kafka dan Spark ke PostgreSQL dan MongoDB dengan latensi median ${n(S.latency_s)} detik, dan agregat hasil *notebook* sama persis dengan agregat yang dimuat Spark. Seluruh ${B.tests.length} pengujian *batch* dan ${S.tests.length} pengujian *streaming* berhasil.`),
  sub("3.2", "Kendala yang Ditemukan"),
  body(`Kendala pertama adalah ukuran data. Tabel data bersih berisi ${n(B.rows)} baris tidak muat di memori *pod* yang dibatasi 4 GiB, sehingga data bersih ditulis sebagai berkas Parquet di area *staging* dan agregasi dibaca dari berkas tersebut. Penghitungan duplikat pada seluruh atribut juga diubah menjadi perbandingan *hash* 64-bit yang hasilnya diperiksa ulang dengan nilai aslinya bila ditemukan *hash* yang sama.`),
  body("Kendala kedua berasal dari sifat sumber data. MTA merevisi data beberapa bulan terakhir setiap minggu, sementara ETL tidak menyimpan data mentah; oleh karena itu, perubahan dikenali dari *fingerprint* harian dan seluruh hari pada bulan yang berubah diekstraksi ulang. Pada GTFS-Realtime, *feed* tidak memiliki atribut keterlambatan, sebagian perjalanan tidak cocok dengan jadwal statis, dan gangguan layanan dari NYCT mencantumkan perjalanan alih-alih rute sehingga rute harus diambil dari perjalanan yang disebutkan."),
  body("Kendala ketiga berkaitan dengan keandalan *streaming*. Pengujian awal menunjukkan dua risiko perhitungan ganda: *snapshot* yang sama dapat terambil dua kali bila MTA belum memperbarui *feed*, dan Spark memutar ulang *micro-batch* terakhir setelah *restart* sehingga *upsert* aditif bisa terjadi dua kali. Keduanya diatasi dengan melewati *snapshot* berulang dan mencatat `batch_id` dalam transaksi yang sama dengan data. Selain itu, versi Python pada *image* Spark lebih lama daripada *image* lain sehingga versi *library* pada kontainer Spark disesuaikan."),
  sub("3.3", "Kesimpulan"),
  body(`Rancangan ETL Minggu 9 berhasil diimplementasikan menjadi *pipeline* yang berjalan di klaster Kubernetes laboratorium. Jalur *batch* memproses ${n(B.rows)} baris data penumpang melalui Extract dari SODA API, Transform dengan DuckDB, dan Load ke PostgreSQL dengan *upsert* per bulan, serta menghasilkan ${n(B.loaded_facts)} baris fakta dan dataset hasil transformasi berisi ${n(B.csv_rows)} baris. Jalur *streaming* memproses data GTFS-Realtime setiap 30 detik melalui Kafka dan Spark ke PostgreSQL dan MongoDB dengan jaminan *exactly-once* untuk agregat keterlambatan. Pemeriksaan konsistensi dan ${B.tests.length + S.tests.length} pengujian menunjukkan bahwa data yang tersimpan sesuai dengan hasil transformasi, sehingga gudang data siap dipakai untuk menganalisis pola penumpang dan keterlambatan kereta.`),
];

const reference = (text) => new Paragraph({
  children: runs(text), alignment: AlignmentType.LEFT,
  indent: { left: 720, hanging: 720 }, spacing: { after: 160, line: 276 },
});
content.push(
  unnumbered("DAFTAR PUSTAKA"),
  reference("Apache Software Foundation. *Apache Kafka Documentation*. https://kafka.apache.org/documentation/"),
  reference("Apache Software Foundation. *Structured Streaming Programming Guide* (Apache Spark 4.2). https://spark.apache.org/docs/latest/streaming/"),
  reference("DuckDB Foundation. *DuckDB Documentation*. https://duckdb.org/docs/"),
  reference("Google. *GTFS Realtime Reference*. https://gtfs.org/documentation/realtime/reference/"),
  reference("Metropolitan Transportation Authority. *MTA Real-Time Data Feeds*. https://api.mta.info"),
  reference("Metropolitan Transportation Authority. *MTA Subway Hourly Ridership: Beginning 2025*. https://data.ny.gov/d/5wq4-mkjj"),
  reference("MongoDB, Inc. *Expire Data from Collections by Setting TTL*. https://www.mongodb.com/docs/manual/tutorial/expire-data/"),
  reference("Reis, J., & Housley, M. (2022). *Fundamentals of Data Engineering*. O'Reilly Media."),
  reference("Varrazzo, D., dkk. *Psycopg 3 Documentation*. https://www.psycopg.org/psycopg3/docs/"),
);

// ---------- Table of contents ----------
fs.writeFileSync(path.join(__dirname, "toc10_entries.json"), JSON.stringify(toc, null, 2));
fs.writeFileSync(path.join(__dirname, "table10_checks.json"), JSON.stringify(tableChecks, null, 2));
const tocPage = [
  new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 360 },
    children: [new TextRun({ text: "DAFTAR ISI", bold: true, size: 28 })] }),
  ...toc.map((e) => new Paragraph({
    tabStops: [{ type: TabStopType.RIGHT, position: CONTENT_W, leader: LeaderType.DOT }],
    indent: { left: [0, 567, 1134][e.level] },
    spacing: { before: e.level ? 0 : 120, after: 40, line: 276 },
    children: [new InternalHyperlink({ anchor: e.anchor, children: [
      ...runs(e.text, { bold: e.level === 0 || undefined }),
      new TextRun({ children: [new Tab(), TOC_PAGES[e.text] || "0"], bold: e.level === 0 || undefined })] })],
  })),
];

const pageFooter = () => new Footer({ children: [new Paragraph({ alignment: AlignmentType.CENTER,
  children: [new TextRun({ children: [PageNumber.CURRENT], size: 22 })] })] });
const page = (start, formatType) => ({ size: PAGE, margin: MARGIN, pageNumbers: { start, formatType } });

function contentSections() {
  const out = [];
  let chunk = [];
  const flush = () => {
    if (!chunk.length) return;
    const first = out.length === 0;
    out.push({ properties: { type: SectionType.NEXT_PAGE,
      page: { size: PAGE, margin: MARGIN, pageNumbers: first ? { start: 1, formatType: NumberFormat.DECIMAL } : { formatType: NumberFormat.DECIMAL } } },
      footers: { default: pageFooter() }, children: chunk });
    chunk = [];
  };
  for (const item of content.flat()) {
    if (item && item.landscape) {
      flush();
      out.push({ properties: { type: SectionType.NEXT_PAGE, verticalAlign: VerticalAlignSection.CENTER,
        page: { size: { ...PAGE, orientation: PageOrientation.LANDSCAPE }, margin: MARGIN, pageNumbers: { formatType: NumberFormat.DECIMAL } } },
        footers: { default: pageFooter() }, children: item.landscape });
    } else {
      chunk.push(item);
    }
  }
  flush();
  return out;
}

const doc = new Document({
  creator: "Kelompok Smile",
  title: "Dokumentasi Implementasi ETL Pipeline Data MTA Subway",
  styles: {
    default: { document: { run: { font: FONT, size: 24 } } },
    paragraphStyles: [
      { id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { font: FONT, size: 28, bold: true, color: "000000" },
        paragraph: { alignment: AlignmentType.CENTER, spacing: { before: 0, after: 480, line: LINE }, keepNext: true, outlineLevel: 0 } },
      { id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { font: FONT, size: 24, bold: true, color: "000000" },
        paragraph: { spacing: { before: 240, after: 120, line: LINE }, keepNext: true, outlineLevel: 1 } },
      { id: "Heading3", name: "Heading 3", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { font: FONT, size: 24, bold: true, italics: false, color: "000000" },
        paragraph: { spacing: { before: 180, after: 100, line: LINE }, keepNext: true, outlineLevel: 2 } },
    ],
  },
  numbering: { config: [
    { reference: "numbers", levels: [{ level: 0, format: LevelFormat.DECIMAL, text: "%1.", alignment: AlignmentType.LEFT,
      style: { paragraph: { indent: { left: 720, hanging: 360 } } } }] },
  ] },
  sections: [
    { properties: { page: { size: PAGE, margin: MARGIN } }, children: cover },
    { properties: { type: SectionType.NEXT_PAGE, page: page(1, NumberFormat.LOWER_ROMAN) },
      footers: { default: pageFooter() }, children: tocPage },
    ...contentSections(),
  ],
});

Packer.toBuffer(doc).then((buf) => { fs.writeFileSync(OUT, buf); console.log(`wrote ${OUT} (${buf.length} bytes)`); });
