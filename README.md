# Proyek Data Integrasi Inovasi - Data Engineer
## Pendahuluan
Git repo ini adalah sebuah program Data pipeline dari studi kasus data penjualan startup e-commerce RetailKita. Program ini akan melalui beberapa bagian:
- Transformasi dan Persiapan Data
- Analisis dan Wawasan
- Pemikiran Strategis

### Transformasi dan Persiapan Data
Di tahap ini, program data pipeline pertama - tama akan menggabungkan semua sumber data menjadi 1 dataframe. Selanjutnya, kolom yang menyimpan data yang belum standard (e.g. kolom  "timeframe" yang menyimpan format data yang tidak standard) akan di standarisasi terlebih dahulu. [link ke ono]

Setelah sumber data sudah menjadi 1 table master data, langkah selanjutnya adalah menyimpan master data ke dalam database. Proyek ini akan menggunakan Postgre yang akan di Dockerize [link docker] sebagai data warehouse. 

### Analisis dan Wawasan
Setelah master data sudah masuk kedalam data warehouse, langkah selanjutnya melakukan query sesuai arahan dari soal. Proses penulisan kode SQL semuannya dilakukan didalam program Python agar program data pipelinne bisa dilakukan dalam sekali jalan (automasi). Kode SQL untuk proses query ini bisa dilihat di file python ini [link python]

### Pemikiran Strategis
Tahap ini adalah analisis lebih lanjut terhadap studi kasus yang sedang dihadapi dalam persoalan ini. Studi kasus disini adalah "retensi pelanggan RetailKita dengan melihat probabilitas churn masing - masing pelanggan. Proses analisa ini bisa dilihat di folder experiment [masukkan link] yang ada didalam file .ipynb

## Cara Menjalankan Program

1. Membuat database postgresql secara docker (pastikan program docker sudah terinstal). Jalankan kode docker-compose.yaml di folder postgre [link ono] dengan kode berikut:

```
docker compose up -d
```
Untuk akses postgre bisa menggunakan Google Chrome dengan alamat `http://localhost:8978/#/admin` dan user dan password:

```
user docker: cbadmin
pass docker: CBAdmin123
```

2. Jalankan program utama data pipeline di folder project [link ono] dan jalankan program `main.py`

```
python main.py
```

Hasil master_data bisa dicek di folder data [link ono]

3. Untuk menjalankan program analisis lebih lanjut mengenai retensi pelanggan RetailKita, bisa di cek di folder experiment [masukin link] dan jalankan didalam file experiment.ipynb

4. Setelah selesai, matikan program postgre yang di Docker dengan mengakses folder postgre [link ono] dan menjalankan kode berikut:

```
docker compose down
```