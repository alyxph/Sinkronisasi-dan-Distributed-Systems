# Naskah Presentasi Demo: Distributed Synchronization System

Berikut adalah rancangan *script* atau naskah presentasi yang bisa kamu gunakan saat merekam video. Naskah ini dibagi menjadi bagian **Visual** (apa yang ditampilkan di layar) dan **Audio** (apa yang kamu bicarakan).

---

## 1. Pembukaan dan Penjelasan Arsitektur (00:00 - 01:00)

**[Visual]**
- Tampilkan slide judul atau file `README.md` bagian atas.
- Tampilkan diagram arsitektur sistem (`docs/architecture.md`).

**[Audio]**
"Halo semuanya, perkenalkan nama saya Alief Rachmattul Islam dengan NIM 11231007. Pada kesempatan kali ini, saya akan mendemonstrasikan tugas mata kuliah Sistem Paralel dan Terdistribusi, yaitu **Distributed Synchronization System**."

"Sistem ini saya bangun dari nol secara *full lokal* tanpa menggunakan database eksternal seperti Redis. Di dalamnya terdapat tiga layanan utama: **Distributed Lock Manager**, **Message Queue**, dan **Distributed Cache**. Sistem ini di-deploy menggunakan Docker Compose dengan total 9 node, yaitu 3 node untuk masing-masing layanan demi memastikan *High Availability* dan *Fault Tolerance*."

---

## 2. Menjalankan Sistem (01:00 - 01:30)

**[Visual]**
- Buka terminal.
- Jalankan perintah `docker compose -f docker/docker-compose.yml up --build -d`.
- Jalankan perintah `docker compose -f docker/docker-compose.yml ps` untuk memperlihatkan semua 9 node (container) sudah berstatus *Up/Running*.

**[Audio]**
"Pertama, mari kita jalankan seluruh sistem. Di sini saya menggunakan Docker Compose untuk membangun dan menjalankan ke-9 node secara bersamaan di *background*. Bisa kita lihat pada output `docker ps`, semua node untuk Lock, Queue, dan Cache sudah berjalan di port-nya masing-masing."

---

## 3. Demo 1: Lock Manager (01:30 - 02:30)

**[Visual]**
- Buka browser, akses Swagger UI Lock Manager di `http://localhost:7001/docs`.
- Buka endpoint `POST /lock/acquire`.
- Masukkan payload JSON untuk *acquire* (contoh: `{"lock": "db_user", "client_id": "client-1", "mode": "exclusive"}`).
- Klik Execute, tunjukkan respons `"status": "granted"`.
- Buka endpoint `POST /lock/release`.
- Masukkan payload yang sama dan Execute.

**[Audio]**
"Oke, sekarang kita masuk ke pengujian pertama, yaitu **Distributed Lock Manager**. Saya membuka antarmuka interaktif Swagger UI di port 7001. Lock Manager ini didukung oleh algoritma konsensus Raft untuk memastikan tidak ada *race condition*."

"Mari kita coba simulasikan klien yang ingin mengambil *lock*. Kita buka bagian `POST /lock/acquire`. Di sini, kita masukkan *Request body* dengan kode JSON seperti ini: `{"lock": "db_user", "client_id": "client-1", "mode": "exclusive"}`. Lalu, kita klik *Execute*."

"Nah, bisa dilihat hasil responsnya adalah `{"status": "granted"}`. Ini artinya klien kita berhasil mendapatkan akses eksklusif ke *resource* 'db_user'. Selama statusnya granted, klien lain tidak bisa mengakses *resource* tersebut. Setelah proses komputasi klien selesai, kita harus melepaskan *lock* agar bisa dipakai yang lain. Kita buka endpoint `POST /lock/release`, masukkan JSON yang sama persis, klik *Execute*, dan responsnya akan menunjukkan *lock* berhasil dilepas."

---

## 4. Demo 2: Message Queue (02:30 - 03:30)

**[Visual]**
- Buka tab baru, akses Swagger UI Queue Node di `http://localhost:7101/docs`.
- Buka endpoint `POST /queue/publish`, masukkan JSON: `{"topic": "orders", "message": "order_123"}`, lalu *Execute*. (Tunjukkan response sukses).
- Buka endpoint `POST /queue/consume`, masukkan JSON: `{"topic": "orders"}`, lalu *Execute*. (Tunjukkan message berhasil diambil).
- Buka endpoint `POST /queue/ack`, masukkan JSON: `{"message_id": "<ID_DARI_RESPONS_SEBELUMNYA>"}`, lalu *Execute*.

**[Audio]**
"Selanjutnya, kita pindah ke port 7101 untuk mendemonstrasikan **Message Queue** terdistribusi. Antrian ini memastikan pesan tidak hilang berkat implementasi penyimpanan lokal via *Write-Ahead Log* (WAL)."

"Pertama, kita jalankan bagian *publish* untuk mengirim pesan. Kita buka `POST /queue/publish`, lalu masukkan kode JSON seperti ini: `{"topic": "orders", "message": "order_123"}`. Kita klik *Execute*, dan hasilnya kita mendapat respons sukses. Ini buat apa? Ini mensimulasikan ada *producer* yang mengirim data pesanan baru ke antrian."

"Sekarang, mari kita ambil pesan tersebut sebagai *consumer*. Kita buka `POST /queue/consume`, ketik kode JSON `{"topic": "orders"}`, dan klik *Execute*. Hasil responsnya akan menampilkan pesan 'order_123' tadi beserta sebuah *message ID* unik. Terakhir, agar pesan ini ditandai selesai dan tidak dikirim ulang, kita buka `POST /queue/ack`, masukkan kode JSON `{"message_id": "<ID_yang_tadi>"}`, lalu *Execute*. Selesai, antrian sudah ter-update."

---

## 5. Demo 3: Distributed Cache (03:30 - 04:30)

**[Visual]**
- Buka tab baru, akses Swagger UI Cache Node di `http://localhost:7201/docs`.
- Buka endpoint `POST /cache/put`, masukkan JSON `{"key": "user_1", "value": "John Doe"}`, lalu *Execute*.
- Buka endpoint `GET /cache/get`, ketikkan `user_1` di parameter key, lalu *Execute*. Tunjukkan bahwa data muncul.

**[Audio]**
"Layanan ketiga adalah **Distributed Cache** di port 7201. Cache ini berfungsi untuk mempercepat akses data sementara dan menggunakan protokol *MESI Coherence* yang menjamin konsistensi data di semua node secara *Peer-to-Peer*."

"Mari kita simpan sebuah data. Kita buka bagian `POST /cache/put`, kemudian kita masukkan kode JSON seperti ini: `{"key": "user_1", "value": "John Doe"}`. Setelah kita klik *Execute*, responsnya mengonfirmasi data tersimpan. Di *background*, operasi ini otomatis memicu *broadcast* ke node lain agar datanya sinkron."

"Untuk membuktikannya, kita ambil data tadi. Buka endpoint `GET /cache/get`, kita cari dengan parameter *key* 'user_1', dan klik *Execute*. Hasil responsnya langsung menampilkan value 'John Doe' dengan sangat cepat karena data ini ditarik langsung dari memori tanpa perlu menyentuh database persisten."

---

## 6. Benchmark Locust (04:30 - 06:00)

**[Visual]**
- Buka terminal, pastikan sistem *virtual environment* Python sudah aktif.
- Jalankan perintah *headless* Locust untuk Lock Manager: 
  `locust -f benchmarks/load_test_scenarios.py LockUser --headless -u 20 -r 5 -t 30s`
- Biarkan *load test* berjalan selama 30 detik.
- Tunjukkan tabel hasil akhir (Requests, Failures, Average Response Time).

*(Opsional: Buka UI Locust di `http://localhost:8089` jika kamu memilih mode UI).*

**[Audio]**
"Sebagai tahap akhir, saya akan melakukan *Load Testing* menggunakan **Locust** untuk melihat ketahanan sistem. Di sini saya menjalankan skenario simulasi 20 pengguna (*users*) yang secara bersamaan melakukan request ke layanan Lock Manager selama 30 detik."

"Bisa kita lihat saat *test* berjalan, sistem menerima ribuan *request*. Dalam sistem terdistribusi, terutama yang menggunakan konsensus seperti Raft, *error* sesaat (transient errors) seperti status 503 adalah hal yang wajar karena adanya proses *leader election*. Namun, skrip ini sudah dikonfigurasi dengan fitur *retry* yang mensimulasikan sistem yang tangguh (*resilient*) dan toleran terhadap kesalahan."

"Dari ringkasan akhir (Terminal Summary), kita bisa melihat total *request* yang berhasil dieksekusi serta rata-rata waktu responsnya (*Response Time*) yang tetap terjaga dan kegagalan yang minim."

---

## 7. Penutup (06:00 - Selesai)

**[Visual]**
- Tampilkan kembali diagram arsitektur.
- Senyum ke kamera (jika menggunakan *facecam*).

**[Audio]**
"Demikianlah demonstrasi dari sistem Lock, Queue, dan Cache terdistribusi yang murni berjalan secara lokal menggunakan HTTP *Peer-to-Peer* dan algoritma konsensus Raft. Secara keseluruhan sistem mampu menangani *request* secara konkuren dan menjaga konsistensi data. Terima kasih atas perhatiannya!"
