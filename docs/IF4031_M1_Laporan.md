# LAPORAN MILESTONE 1

## Platform Koordinasi Kebencanaan BMKG–PVMBG–BNPB

**Mata kuliah:** IF4031 Arsitektur Aplikasi Terdistribusi  
**Milestone:** 1 — Sistem Terdistribusi Dasar  
**Nama kelompok:** `[ISI NAMA KELOMPOK]`  
**Tanggal:** `[ISI TANGGAL FINALISASI]`

### Anggota kelompok

| No. | Nama | NIM | Peran utama |
|---:|---|---|---|
| 1 | `[ISI]` | `[ISI]` | `[ISI]` |
| 2 | `[ISI]` | `[ISI]` | `[ISI]` |
| 3 | `[ISI]` | `[ISI]` | `[ISI]` |
| 4 | `[ISI]` | `[ISI]` | `[ISI]` |

> **Catatan finalisasi:** Ganti seluruh placeholder `[ISI ...]`, tambahkan screenshot hasil demo, lalu render dokumen ini ke PDF dengan nama `IF4031_M1_<NamaKelompok>.pdf`.

## Tabel kontribusi

| Anggota | Perancangan | Implementasi | Pengujian/demo | Penulisan |
|---|---|---|---|---|
| `[Nama 1]` | `[Kontribusi]` | `[Kontribusi]` | `[Kontribusi]` | `[Kontribusi]` |
| `[Nama 2]` | `[Kontribusi]` | `[Kontribusi]` | `[Kontribusi]` | `[Kontribusi]` |
| `[Nama 3]` | `[Kontribusi]` | `[Kontribusi]` | `[Kontribusi]` | `[Kontribusi]` |
| `[Nama 4]` | `[Kontribusi]` | `[Kontribusi]` | `[Kontribusi]` | `[Kontribusi]` |

---

# 1. Pendahuluan

## 1.1 Latar belakang dan tujuan

### 1.1.1 Konteks koordinasi BMKG, PVMBG, dan BNPB

Penanganan bencana memerlukan informasi dari berbagai instansi yang memiliki sumber data, kontrak layanan, dan karakteristik operasional masing-masing. Dalam cakupan Milestone 1, BMKG menyediakan informasi seismik dan peringatan tsunami, sedangkan PVMBG menyediakan laporan aktivitas gunung api. BNPB berperan sebagai node koordinasi yang mengumpulkan dan menyatukan informasi tersebut untuk digunakan oleh client downstream.

### 1.1.2 Masalah yang diselesaikan

Perbedaan antarlayanan menimbulkan beberapa tantangan dalam sistem terdistribusi. Skema data dapat berubah tanpa koordinasi dengan Aggregator, waktu respons tiap sumber berbeda, dan salah satu sumber dapat mengalami gangguan sementara. Selain itu, akses dari instansi sumber dan client downstream perlu menggunakan kredensial serta hak akses yang terpisah. Sistem juga perlu menyimpan data dengan atribut yang dapat berkembang dan menyampaikan event kepada beberapa consumer secara independen.

### 1.1.3 Tujuan sistem

Proyek ini membangun prototipe platform koordinasi kebencanaan yang menghubungkan mock service BMKG dan PVMBG dengan komponen BNPB. Sistem dirancang untuk memetakan data sumber ke format kanonik HazardEvent, mengambil data melalui polling berkala, dan menyediakan akses downstream dengan hak akses yang ditegakkan oleh server.

Rancangan dan implementasi juga mencakup penanganan perubahan skema yang bersifat aditif, isolasi dan degradasi layanan saat sumber mengalami gangguan, pemisahan domain kredensial, deployment service secara independen, serta distribusi event melalui message broker. Laporan ini menjelaskan rancangan tersebut dan menyajikan bukti pengujian untuk kriteria Milestone 1.

## 1.2 Ruang lingkup dan batasan M1

### 1.2.1 Cakupan sumber dan jenis bencana

Sistem mencakup dua mock service sumber, yaitu BMKG dan PVMBG, serta BNPB sebagai node koordinasi. Data yang diproses meliputi kejadian seismik dan peringatan tsunami dari BMKG serta laporan aktivitas gunung api dari PVMBG. Aggregator BNPB mengambil data dari kedua mock melalui polling berkala; mock tidak mengirimkan data secara langsung kepada BNPB.

### 1.2.2 Cakupan client dan fitur

Client downstream yang dicakup adalah Media, Tim Lapangan, dan BNPB Ops. Ketiganya menggunakan identitas dan kredensial berbeda. Hak akses Media dibatasi pada data ringkasan, sedangkan Tim Lapangan dan BNPB Ops dapat mengakses data mentah sesuai scope yang diberikan. Seluruh client bersifat read-only pada Milestone 1.

Sistem juga mencakup penyimpanan HazardEvent kanonik, autentikasi dan otorisasi client, health check dan pencatatan correlation ID, serta distribusi event kepada beberapa consumer.

### 1.2.3 Batasan Milestone 1

Implementasi menggunakan mock service BMKG dan PVMBG, sehingga integrasi langsung dengan layanan produksi kedua instansi tidak termasuk dalam cakupan. Jalur pelaporan dari lapangan ke BNPB dan jenis bencana di luar seismik serta vulkanik juga tidak dibahas.

Perubahan skema yang dicakup adalah penambahan field. Field baru dapat ditangani sebagai atribut dinamis; perubahan nama atau tipe field dan penghapusan field wajib tidak termasuk dalam dukungan perubahan skema otomatis.

---

# 2. Deskripsi umum sistem

## 2.1 Gambaran komponen dan tanggung jawab

### 2.1.1 Layanan sumber

BMKG dan PVMBG disimulasikan sebagai mock service yang berjalan dalam container terpisah. BMKG menyediakan data kejadian seismik dan peringatan tsunami melalui endpoint REST. Peringatan tsunami dikaitkan dengan kejadian seismik menggunakan `related_event_id`. Mock BMKG memiliki kredensial akses sendiri dan karakteristik waktu respons yang rendah serta konsisten.

PVMBG menyediakan laporan aktivitas gunung api melalui endpoint REST dengan kredensial yang berbeda dari BMKG. Mock ini juga menyediakan kendali untuk mengaktifkan field `confidence_level` saat sistem berjalan dan untuk memicu atau mengakhiri outage. Waktu respons PVMBG lebih tinggi dan bervariasi untuk merepresentasikan karakteristik sumber yang lebih lambat.

### 2.1.2 Komponen koordinasi BNPB

Aggregator melakukan polling berkala ke BMKG dan PVMBG, memetakan respons sumber ke HazardEvent, serta menyimpan data kanonik. Aggregator juga menyediakan API internal melalui gRPC untuk akses data dan status sumber. Canonical Store hanya dimiliki dan diakses langsung oleh Aggregator.

Client API menjadi pintu masuk HTTP bagi client downstream. Komponen ini meneruskan operasi autentikasi ke Auth Service, memeriksa scope, meminta data kepada Aggregator, lalu mengembalikan proyeksi data yang sesuai dengan hak akses client. Client API tidak membaca Canonical Store secara langsung.

Auth Service mengelola identitas client, penerbitan token, refresh token, dan validasi akses. Penyimpanan yang digunakan Auth Service berada dalam tanggung jawab layanan tersebut dan terpisah secara kepemilikan dari Canonical Store Aggregator.

### 2.1.3 Storage, broker, dan consumer

PostgreSQL digunakan untuk menyimpan HazardEvent kanonik beserta atribut dinamisnya. Aggregator menulis dan membaca Canonical Store. Auth Service mengakses penyimpanannya sendiri untuk kebutuhan autentikasi.

Aggregator menggunakan transactional outbox untuk menjaga konsistensi antara penyimpanan HazardEvent dan pencatatan event yang perlu dipublikasikan. Outbox worker menerbitkan event ke RabbitMQ. Dashboard consumer dan Notifier consumer berlangganan secara independen; Portal consumer dapat dijalankan sebagai consumer tambahan. Dengan demikian, Aggregator tidak perlu memanggil setiap consumer secara langsung.

### 2.1.4 Client downstream dan komponen demonstrasi

Client downstream terdiri atas Media, Tim Lapangan, dan BNPB Ops. Media memperoleh data ringkasan, sedangkan Tim Lapangan dan BNPB Ops dapat memperoleh data mentah sesuai scope token masing-masing. Seluruh client hanya membaca data pada Milestone 1.

Demo Console menyediakan antarmuka untuk menjalankan skenario dan melihat bukti dari permintaan HTTP. Demo API mendukung operasi demonstrasi, pemeriksaan service, dan pembacaan artifact pengujian. Keduanya merupakan komponen pendukung demonstrasi dan tidak mengakses Canonical Store secara langsung.

## 2.2 Data, kontrak, dan endpoint utama

### 2.2.1 Data sumber dan format kanonik

BMKG menyediakan `SeismicEvent` dan `TsunamiWarning`, sedangkan PVMBG menyediakan `VolcanicReport`. Aggregator menggabungkan informasi sumber yang berkaitan dan memetakannya ke HazardEvent sebagai representasi kanonik BNPB. Field ringkasan digunakan untuk kebutuhan umum client. Koordinat presisi dan atribut khusus sumber diklasifikasikan sebagai data mentah, sehingga hanya client dengan scope yang sesuai dapat menerimanya.

### 2.2.2 Port dan endpoint

Tabel berikut merangkum port dan endpoint utama yang digunakan pada konfigurasi lokal. Nilai port dapat disesuaikan melalui konfigurasi sebagaimana didokumentasikan di README.

| Service | Port lokal | Endpoint atau antarmuka utama |
|---|---:|---|
| BMKG mock | 8081 | `GET /seismic-events?since=<timestamp>`, `GET /tsunami-warnings?since=<timestamp>`, `GET /health` |
| PVMBG mock | 8082 | `GET /volcanic-reports?since=<timestamp>`, `POST /admin/schema-version`, `POST /admin/outage`, `GET /health` |
| Client API | 8080 | `POST /auth/token`, `POST /auth/refresh`, `GET /hazards`, `GET /sources/status`, `GET /health` |
| Aggregator | 8090 dan 9090 | HTTP health check dan API internal gRPC |
| Auth Service | 9091 | API autentikasi internal gRPC |
| Demo API | 8086 | API kendali demonstrasi, probe service, dan artifact |
| Demo Console | 3000 | Antarmuka web untuk demonstrasi |

Endpoint sumber yang menerima parameter waktu mendukung pengambilan data sejak posisi polling sebelumnya. BMKG dan PVMBG masing-masing memvalidasi kredensial yang ditetapkan untuk instansinya. Endpoint administratif PVMBG digunakan operator untuk memicu perubahan skema dan outage selama demonstrasi.

### 2.2.3 Kepemilikan storage dan batas akses

Canonical Store dimiliki Aggregator. Client API, Auth Service, demo-api, dan consumer tidak terhubung langsung ke penyimpanan kanonik. Client API meminta data melalui API gRPC Aggregator. Auth Service menggunakan penyimpanannya sendiri untuk mengelola data autentikasi. Batas ini menjaga kepemilikan data dan akses antarservice tetap eksplisit.

## 2.3 Arsitektur dan alur komunikasi

### 2.3.1 Arsitektur sistem

Komunikasi antara sumber dan Aggregator menggunakan REST. Client API berkomunikasi dengan Aggregator dan Auth Service menggunakan gRPC. Setelah HazardEvent tersimpan, outbox worker menerbitkannya ke RabbitMQ untuk diterima consumer melalui queue masing-masing. Setiap service dijalankan dalam container tersendiri.

![Gambar 1. Arsitektur sistem dan alur komunikasi](https://storage.googleapis.com/second-petal-295822.appspot.com/elements/elements%3Abf5e8017f614dcfeba9b7aa5e4e3c8c935b3f05afccc60fcd7c0032671ad815e.png)

[Lihat dan edit diagram Gambar 1 di Eraser](https://app.eraser.io/workspace/LNBH6Hy1uRmyFxbz4AFM?diagram=xaim1SLg5aFtyOHBfDu2&layout=canvas)

### 2.3.2 Alur permintaan client downstream

Client mengirimkan kredensialnya melalui Client API untuk memperoleh token. Client API meneruskan permintaan penerbitan token kepada Auth Service. Pada permintaan data berikutnya, Client API memvalidasi token dan scope, lalu meminta HazardEvent kepada Aggregator. Aggregator membaca Canonical Store dan mengembalikan data melalui gRPC. Client API kemudian menyusun respons ringkasan atau respons yang mencakup field mentah sesuai hak akses.

![Gambar 2. Urutan autentikasi dan permintaan data client](https://storage.googleapis.com/second-petal-295822.appspot.com/elements/elements%3A36bbe42700e680a0877c4a80542312318f2a03f9b59484df5122b5a97265d15d.png)

[Lihat dan edit diagram Gambar 2 di Eraser](https://app.eraser.io/workspace/LNBH6Hy1uRmyFxbz4AFM?diagram=Dkj8HCF8_4DvqY_LuX7d&layout=canvas)

### 2.3.3 Alur polling, penyimpanan, dan distribusi event

Aggregator menjalankan siklus polling untuk BMKG dan PVMBG. Pada siklus BMKG, Aggregator mengambil data seismik dan peringatan tsunami dari endpoint terpisah, lalu menghubungkan keduanya menggunakan referensi kejadian. Pada siklus PVMBG, Aggregator mengambil laporan gunung api. Kedua alur diproses secara independen agar keterlambatan satu sumber tidak menahan pengambilan data dari sumber lainnya.

Setelah data dipetakan menjadi HazardEvent, Aggregator menyimpan event dan catatan outbox dalam transaksi database. Outbox worker mengambil catatan yang belum dipublikasikan dan mengirim event ke RabbitMQ. Broker meneruskan event ke queue setiap consumer. Consumer mengakui pesan setelah pemrosesan sesuai konfigurasi pengiriman.

![Gambar 3. Polling sumber, persistensi event, dan fan-out](https://storage.googleapis.com/second-petal-295822.appspot.com/elements/elements%3Ae4e2d93fcd6936e9d40c847c4fb3ff10a2e3daf61e4df9e4ca546637b77db076.png)

[Lihat dan edit diagram Gambar 3 di Eraser](https://app.eraser.io/workspace/LNBH6Hy1uRmyFxbz4AFM?diagram=midk0YWpJpJhZVVf1QDb&layout=canvas)

## 2.4 Topologi deployment

### 2.4.1 Orkestrasi Docker Compose

Pada lingkungan lokal, service dijalankan sebagai container terpisah dan diorkestrasi melalui Docker Compose. Konfigurasi ini digunakan untuk menjalankan keseluruhan sistem, menguji health check, dan memicu skenario yang membutuhkan penghentian, pembangunan ulang, atau penyalakan kembali service tertentu. Demo Console dan Demo API dapat diaktifkan melalui profil demonstrasi.

### 2.4.2 Deployment lokal dengan K3s dan K3d

Untuk pengujian deployment berbasis Kubernetes, K3d menyediakan cluster K3s lokal. Workload service dan consumer didefinisikan melalui manifest Kustomize. Client API dapat dijalankan dengan beberapa replica di belakang service Kubernetes, sedangkan komponen penyimpanan dijalankan dengan dukungan volume persisten sesuai konfigurasi deployment.

![Gambar 4. Topologi deployment lokal K3d](https://storage.googleapis.com/second-petal-295822.appspot.com/elements/elements%3A5d7d5bc9f6484281f5f6691ff81c7113425797bb18152540882c27f8c7351481.png)

[Lihat dan edit diagram Gambar 4 di Eraser](https://app.eraser.io/workspace/LNBH6Hy1uRmyFxbz4AFM?diagram=eCwmCAkSK__P3UMjd5Xo&layout=canvas)

---

# 3. Teknologi dan alasan pemilihan

Teknologi dipilih berdasarkan kontrak komunikasi antarlayanan, kebutuhan penyimpanan data kanonik dan atribut dinamis, serta kebutuhan menjalankan setiap komponen secara independen. Pilihan ini digunakan untuk mendukung perilaku yang disyaratkan pada M1; alasan dan konsekuensi khusus untuk perubahan skema, storage, dan distribusi event dibahas lebih lanjut pada bagian problem terkait.

## 3.1 Bahasa pemrograman dan framework

### 3.1.1 Go

Go digunakan untuk mock BMKG dan PVMBG serta service BNPB, termasuk Aggregator, Auth Service, Client API, consumer, dan Demo API. Bahasa ini menyediakan dukungan konkurensi dan pustaka jaringan yang sesuai untuk service yang berkomunikasi melalui HTTP, gRPC, dan RabbitMQ. Setiap service dapat dibangun menjadi executable tersendiri dan dikemas sebagai container secara independen.

### 3.1.2 Gin dan React

Gin digunakan pada boundary transport HTTP untuk routing dan middleware pada service yang menyediakan endpoint HTTP. Pemrosesan domain dan application tetap berada di luar handler transport. Demo Console dibangun menggunakan React dan TypeScript untuk menyediakan antarmuka bagi operator saat menjalankan skenario dan memeriksa bukti demonstrasi.

## 3.2 Protokol dan format komunikasi

### 3.2.1 REST dan JSON untuk mock source

Aggregator berkomunikasi dengan BMKG dan PVMBG melalui REST dan JSON. Pilihan ini sesuai dengan kontrak endpoint mock yang ditetapkan dalam spesifikasi, serta memungkinkan setiap sumber mempertahankan kredensial dan perilaku layanannya sendiri.

### 3.2.2 gRPC dan Protobuf untuk komunikasi internal

Client API berkomunikasi dengan Aggregator dan Auth Service melalui gRPC dengan kontrak Protobuf. Kontrak tersebut mendefinisikan operasi dan bentuk data internal secara eksplisit. Client API dapat meminta data kanonik melalui API Aggregator tanpa mengakses Canonical Store secara langsung.

### 3.2.3 AMQP untuk distribusi event

Aggregator menerbitkan HazardEvent ke RabbitMQ melalui AMQP. Broker mengarahkan event ke queue consumer yang terpisah. Dengan pola ini, producer tidak perlu mengetahui implementasi setiap consumer. Semantik pengiriman dan dampaknya terhadap idempotensi dijelaskan lebih rinci pada Problem 5.

## 3.3 Teknologi penyimpanan

### 3.3.1 PostgreSQL, pgx, dan JSONB

PostgreSQL digunakan untuk menyimpan data kanonik Aggregator. Kolom utama HazardEvent tetap memiliki struktur yang jelas, sementara atribut khusus sumber disimpan dalam JSONB. pgx digunakan sebagai driver PostgreSQL pada service yang memerlukan akses database. Auth Service memiliki tanggung jawab atas penyimpanan autentikasinya sendiri, terpisah dari kepemilikan Canonical Store Aggregator.

Kombinasi kolom relasional dan JSONB mempertahankan struktur data kanonik sekaligus menerima atribut tambahan seperti `confidence_level` tanpa perubahan skema database. Aturan validasi dan pemetaan atribut tetap ditangani oleh aplikasi.

### 3.3.2 Alternatif penyimpanan

Penyimpanan relasional dengan kolom tetap akan menjaga tipe data dan query tetap sederhana, tetapi field baru dapat memerlukan perubahan skema. Document store lebih lentur terhadap bentuk dokumen yang beragam, tetapi akan menambah teknologi penyimpanan dan pola query lain ke dalam sistem. PostgreSQL dengan JSONB dipilih sebagai kompromi: field kanonik tetap terstruktur dan atribut dinamis dapat disimpan tanpa migrasi ketika field tambahan muncul.

## 3.4 Broker dan alternatif distribusi event

RabbitMQ dipilih untuk mendistribusikan HazardEvent ke beberapa consumer melalui queue yang durable dan terpisah. Kafka juga dapat menyediakan distribusi event dan kemampuan replay, tetapi kebutuhan M1 lebih menekankan fan out ke consumer independen dengan operasi lokal yang sederhana. RabbitMQ lebih sesuai untuk kebutuhan tersebut. Konsekuensi semantik pengiriman dan kewajiban idempotensi consumer dibahas pada Problem 5.

## 3.5 Orkestrasi dan pengujian

### 3.5.1 Docker Compose

Docker Compose digunakan untuk menjalankan stack lokal melalui satu konfigurasi orkestrasi. Setiap service tetap memiliki container tersendiri, sehingga dapat dihentikan atau dibangun ulang secara independen untuk demonstrasi Problem 4.

### 3.5.2 K3s, K3d, dan Kustomize

K3d digunakan untuk menyediakan cluster K3s lokal. Manifest Kustomize mengelola konfigurasi dasar dan overlay deployment. Kombinasi ini digunakan untuk menguji service pada lingkungan Kubernetes lokal, termasuk menjalankan beberapa replica Client API.

### 3.5.3 k6

k6 digunakan untuk pengujian beban berkelanjutan dan pengukuran throughput, percentile latensi, serta error rate. Konfigurasi dan hasil pengujian yang menjadi bukti Problem 2 disajikan pada bagian tersebut.

---

# 4. Asumsi dan kebijakan desain

Bagian ini merangkum asumsi operasional yang menjadi dasar rancangan serta kebijakan yang diterapkan lintas komponen. Nilai yang disebut sebagai default berasal dari konfigurasi repository dan dapat diubah melalui environment sesuai kebutuhan pengujian.

## 4.1 Asumsi sistem

### 4.1.1 Perilaku sumber data

BMKG dan PVMBG diperlakukan sebagai layanan otonom yang hanya merespons permintaan. Keduanya tidak mengirimkan data secara langsung ke BNPB. Aggregator mengambil data melalui polling berkala dengan interval yang dapat dikonfigurasi. Nilai default `POLL_INTERVAL` adalah 3 detik. Mock menyediakan 20 record historis secara default melalui `SEED_COUNT`, dan menghasilkan data baru dengan interval default 10 detik.

### 4.1.2 Ruang lingkup data dan client

Milestone 1 mencakup data seismik dan peringatan tsunami dari BMKG serta laporan aktivitas gunung api dari PVMBG. Client downstream yang disimulasikan adalah Media, Tim Lapangan, dan BNPB Ops. Seluruh client hanya membaca data; pelaporan dari lapangan ke BNPB berada di luar cakupan.

### 4.1.3 Konfigurasi dan lingkungan

Nilai port, kredensial, interval polling, dan konfigurasi runtime dibaca dari environment. `.env.example` menyediakan nama variabel serta nilai contoh. Nilai rahasia untuk penggunaan lokal dimasukkan ke konfigurasi lokal yang tidak di-commit. Konfigurasi yang sama dapat disesuaikan untuk menjalankan stack melalui Docker Compose atau deployment Kubernetes lokal.

## 4.2 Kebijakan desain

### 4.2.1 Perubahan skema dan penyimpanan

Perubahan skema yang ditangani otomatis adalah penambahan field. Field tambahan yang tidak memiliki padanan kanonik diteruskan ke `attributes`. Perubahan nama atau tipe field, maupun penghilangan field wajib, memerlukan penanganan eksplisit dan tidak dianggap kompatibel secara otomatis.

Canonical Store menjadi sumber kebenaran bagi HazardEvent yang sudah diterima BNPB. Store tersebut dimiliki Aggregator dan hanya diakses langsung oleh Aggregator. Service lain meminta data melalui API pemiliknya. Auth Service memiliki tanggung jawab atas penyimpanan data autentikasinya sendiri.

### 4.2.2 Ketersediaan dan kebaruan data

Kegagalan satu sumber tidak boleh menghilangkan akses terhadap data dari sumber lain. Data yang telah tersimpan tetap dapat digunakan selama sumber sedang tidak tersedia, dan status sumber ditandai `stale` dengan informasi `stale_since`. Sistem membedakan kondisi tersebut dari kegagalan internal yang tidak terduga.

### 4.2.3 Kredensial dan hak akses

Kredensial BMKG, PVMBG, dan setiap client downstream merupakan identitas yang terpisah. Access token client memiliki TTL yang dapat dikonfigurasi dengan nilai default 60 detik. Hak akses diperiksa di sisi BNPB sebelum data dikembalikan. Media hanya menerima field ringkasan, sedangkan akses ke field mentah memerlukan scope yang sesuai.

### 4.2.4 Pengiriman event dan duplikasi

Publikasi event menggunakan transactional outbox dan RabbitMQ dengan semantik at least once. Karena itu, event dapat diterima ulang dan consumer perlu mengenali duplikasi berdasarkan `hazard_id`. Implementasi consumer menyimpan identitas event yang telah diproses di memori; deduplikasi berlaku selama proses berjalan. Setelah consumer dimulai ulang, event yang dikirim ulang dapat diproses kembali. Dampak dan bukti perilaku tersebut dibahas pada Problem 5.

---

# 5. Problem 1 — Arsitektur dan interoperabilitas data

## 5.1 Rancangan solusi

### 5.1.1 Pemetaan data sumber ke HazardEvent

Aggregator menerjemahkan data BMKG dan PVMBG ke format kanonik HazardEvent. Pemetaan field inti dilakukan secara eksplisit. Data khusus sumber yang tidak memiliki padanan kanonik ditempatkan pada `attributes`.

Untuk data BMKG, `event_id` menjadi `source_ref_id`, jenis bahaya ditetapkan sebagai `SEISMIC`, dan `region_name` menjadi `area_name`. Koordinat episentrum dipetakan ke `latitude` dan `longitude`, sedangkan waktu kejadian dipetakan ke `occurred_at`. Aggregator mengambil `SeismicEvent` dan `TsunamiWarning` melalui endpoint terpisah lalu menghubungkan keduanya menggunakan `related_event_id`. Jika ada peringatan tsunami, tingkat ancamannya menentukan severity kanonik. Jika tidak ada peringatan, severity ditentukan berdasarkan magnitude: di bawah 5,0 menjadi `NORMAL`, mulai 5,0 hingga di bawah 6,5 menjadi `WASPADA`, dan 6,5 atau lebih menjadi `SIAGA`. Nilai magnitude, depth, potensi tsunami, serta atribut peringatan disimpan dalam `attributes`.

Untuk data PVMBG, `report_id` menjadi `source_ref_id` dan jenis bahaya ditetapkan sebagai `VOLCANIC`. `volcano_id` dipetakan melalui tabel referensi gunung api milik BNPB untuk memperoleh nama area dan koordinat. `reported_at` menjadi `occurred_at`, sedangkan `alert_level` dipetakan ke severity kanonik. Jumlah letusan, tinggi kolom abu, dan field tambahan seperti `confidence_level` disimpan dalam `attributes`.

### 5.1.2 Penanganan perubahan skema

Payload PVMBG diproses dengan pemetaan dinamis agar field tambahan tetap tersedia bagi Aggregator. Field kanonik yang dikenal dipetakan ke kolom HazardEvent. Field lain yang belum memiliki padanan kanonik diteruskan ke `attributes` JSONB. Dengan kebijakan ini, penambahan `confidence_level` tidak memerlukan perubahan kode atau migrasi database selama service berjalan.

## 5.2 Alasan keputusan dan batasan

Pemetaan dinamis dipilih agar Aggregator tidak terikat pada daftar field sumber yang dianggap tetap. Kolom kanonik tetap bertipe dan divalidasi, sementara JSONB menampung atribut sumber yang dapat bertambah. Pendekatan ini mempertahankan kemampuan query pada field inti sekaligus menghindari migrasi untuk field tambahan.

Kebijakan kompatibilitas dibatasi pada perubahan aditif. Field baru diteruskan ke `attributes`. Field wajib yang hilang atau berubah tipe ditangani sebagai kegagalan pemetaan untuk record tersebut. Perubahan nama field dan perubahan makna field tidak ditebak atau diterapkan otomatis. Record dari skema lama dan baru dapat tetap disimpan serta dibaca berdampingan.

Konsekuensinya, kode pemetaan tetap harus mengenali dan memvalidasi field yang menjadi bagian dari kontrak kanonik. Query terhadap atribut dinamis juga memerlukan aturan yang eksplisit pada lapisan aplikasi.

## 5.3 Pengujian dan bukti penyelesaian

Pengujian P1 mencakup pemetaan awal data BMKG dan PVMBG, perubahan skema PVMBG ketika sistem berjalan, serta pembacaan HazardEvent setelah field baru muncul. Kasus end to end `TestP1CanonicalMappingForBothSources` memeriksa field kanonik dan atribut sumber untuk kedua instansi. Test `TestSeismicSeverityMapping` memeriksa aturan severity berdasarkan magnitude dan tingkat peringatan, `TestWarningsByEventIDCorrelatesUsingRelatedEventID` memeriksa korelasi warning dengan kejadian seismik, dan `TestVolcanicPreservesAdditiveFields` memeriksa pemetaan referensi gunung api serta field tambahan. `TestP1SchemaEvolutionKeepsPVMBGHazardsValid` memeriksa kemunculan field baru tanpa restart.

Demonstrasi dimulai dengan mengambil data PVMBG pada skema awal dan memastikan HazardEvent valid untuk kedua sumber. Operator kemudian memanggil `POST /admin/schema-version` untuk mengaktifkan `confidence_level`. Aggregator tetap berjalan tanpa restart, menerima field tersebut pada polling berikutnya, lalu menyimpannya dalam `attributes`. Perbandingan record sebelum dan sesudah perubahan menunjukkan bahwa kedua bentuk skema tetap dapat dibaca.

> **[PLACEHOLDER GAMBAR P1.1: Contoh HazardEvent kanonik hasil pemetaan awal dari BMKG dan PVMBG.]**

> **[PLACEHOLDER GAMBAR P1.2: Pemanggilan endpoint perubahan skema PVMBG dan bukti bahwa service tidak direstart.]**

> **[PLACEHOLDER GAMBAR P1.3: Perbandingan HazardEvent PVMBG sebelum dan sesudah perubahan, termasuk `attributes.confidence_level`, disertai log berkorelasi.]**

Artifact [P1 canonical mapping](../artifacts/p1-canonical-mapping.json) mencatat 90 HazardEvent BMKG dan 90 HazardEvent PVMBG dengan pemetaan kanonik valid. Artifact [P1 schema evolution](../artifacts/p1-schema-evolution.json) mencatat 25 record tanpa `confidence_level` dan 64 record dengan field tersebut. Artifact [P4 JSONB coexistence](../artifacts/p4-jsonb-coexistence.json) merekam hasil yang sama untuk kriteria penyimpanan tanpa migrasi.

---

# 6. Problem 2 — Concurrency, availability, dan graceful degradation

## 6.1 Rancangan solusi

### 6.1.1 Konkurensi dan isolasi sumber

Aggregator menjalankan siklus polling BMKG dan PVMBG secara independen. Keterlambatan atau kegagalan pada satu siklus tidak menghentikan siklus sumber lainnya. Panggilan keluar dibatasi oleh timeout. Pada Client API, jumlah request yang diproses bersamaan dibatasi dan setiap request memiliki timeout agar lonjakan beban tidak menghabiskan resource tanpa kendali.

### 6.1.2 Degradasi saat sumber tidak tersedia

Jika polling PVMBG gagal, Aggregator mencatat status sumber sebagai `stale` dan mengisi `stale_since`. Data BMKG tetap dapat dibaca. Data PVMBG yang sudah tersimpan tetap tersedia dengan status kebaruan yang dapat diperiksa oleh client. Kegagalan sumber ditangani sebagai status sumber, bukan sebagai kegagalan internal yang tidak terbedakan.

## 6.2 Alasan keputusan dan batasan

Polling independen menghindari head of line blocking antara sumber dengan karakteristik latensi berbeda. Timeout membatasi lamanya satu panggilan keluar, sedangkan batas inflight request pada Client API melindungi kapasitas layanan saat terjadi lonjakan akses.

Penggunaan data terakhir PVMBG menjaga sebagian fungsi tetap tersedia selama outage, tetapi data tersebut bukan data real time. Karena itu respons perlu dibaca bersama status `stale` dan `stale_since`. Batas inflight juga dapat menghasilkan penolakan terkontrol ketika kapasitas tercapai; jumlah penolakan tersebut perlu dilaporkan terpisah dari error tak terduga.

## 6.3 Pengujian dan bukti penyelesaian

### 6.3.1 Pengujian beban dan isolasi sumber

Pengujian isolasi dijalankan menggunakan k6 dengan 25 virtual user untuk permintaan BMKG saja dan 25 virtual user untuk permintaan PVMBG selama 30 detik. Stack menggunakan `PVMBG_MAX_DELAY=3s`. Dari 27.558 request, throughput tercatat 914,981 request per detik. Latensi BMKG saja adalah p50 3,445 milidetik, p95 8,790 milidetik, dan p99 19,462 milidetik. Latensi p95 PVMBG adalah 8,425 milidetik. Error rate dan penolakan terkontrol sama sama 0. Dengan demikian, p95 BMKG saja berada di bawah ambang 300 milidetik saat PVMBG diberi delay hingga 3 detik.

Pengujian sustained menggunakan executor `constant-vus`, 50 virtual user selama 60 detik, dan jeda 100 milidetik pada setiap iterasi. Endpoint yang diuji adalah `GET /hazards?source=BMKG`. Run terbaru menghasilkan 28.211 request dengan throughput 468,899 request per detik. Latensi tercatat p50 4,888 milidetik, p95 13,970 milidetik, dan p99 19,915 milidetik. Error rate dan penolakan terkontrol sama sama 0, sehingga memenuhi ambang error tak terduga di bawah 1 persen.

Hasil mesin tersedia pada [artifact isolasi](../artifacts/p2-isolation.json) dan [artifact sustained](../artifacts/p2-sustained.json). Nilai ini berlaku untuk stack lokal dan konfigurasi yang digunakan pada run tersebut.

### 6.3.2 Pengujian outage dan pemulihan

Kasus end to end `TestP2PVMBGOutageKeepsBMKGAvailable` memicu outage PVMBG, menunggu status berubah menjadi `stale`, lalu memastikan permintaan data BMKG dan PVMBG tetap memperoleh respons. Setelah outage dimatikan, pengujian menunggu status PVMBG kembali menjadi `healthy` tanpa restart service.

Untuk demonstrasi outage, operator memicu `POST /admin/outage`, memeriksa `stale_since`, lalu mematikan outage dan memastikan status pulih. Run E2E menghasilkan [artifact outage](../artifacts/p2-outage.json) yang merekam status stale, kelanjutan layanan BMKG, respons data PVMBG yang tersimpan, dan pemulihan tanpa restart.

> **[PLACEHOLDER GAMBAR P2.1: Konfigurasi PVMBG dengan delay 3 detik dan hasil p95 permintaan BMKG saja.]**

> **[PLACEHOLDER GAMBAR P2.2: Status PVMBG stale beserta `stale_since`, respons BMKG saat outage, dan status PVMBG setelah pulih.]**

> **[PLACEHOLDER GAMBAR P2.3: Ringkasan load test sustained yang menampilkan jumlah virtual user, durasi, throughput, p50, p95, p99, dan error rate.]**

---

# 7. Problem 3 — Service-to-service authentication dan trust

## 7.1 Rancangan solusi

### 7.1.1 Pemisahan domain kredensial

Setiap instansi sumber dan client downstream memiliki identitas serta kredensialnya sendiri. BMKG memvalidasi API key melalui header `X-BMKG-Key`, sedangkan PVMBG memvalidasi bearer token melalui header `Authorization`. Auth Service mengelola kredensial Media, Tim Lapangan, dan BNPB Ops. Service internal BNPB menggunakan token internal yang divalidasi pada boundary gRPC.

Kredensial dari satu domain tidak diterima oleh domain lain. Rincian pemilik validasi dirangkum pada tabel berikut.

| Domain | Kredensial | Pemilik validasi |
|---|---|---|
| BMKG | `X-BMKG-Key` | BMKG |
| PVMBG | `Authorization: Bearer ...` | PVMBG |
| Internal BNPB | `INTERNAL_SERVICE_TOKEN` | Interceptor gRPC service terkait |
| Media | Secret `media-client` | Auth Service |
| Tim Lapangan | Secret `field-team-client` | Auth Service |
| BNPB Ops | Secret `bnpb-ops-client` | Auth Service |

### 7.1.2 Scope dan proyeksi data

Auth Service menerbitkan access token dengan scope sesuai identitas client. Client API memvalidasi token melalui Auth Service dan menegakkan scope di sisi server. Media hanya memperoleh field ringkasan; permintaan `raw=true` ditolak dengan HTTP 403. Tim Lapangan dan BNPB Ops dapat mengakses field mentah sesuai scope.

Access token memiliki TTL yang dapat dikonfigurasi dengan nilai default 60 detik. Refresh token disimpan dalam bentuk hash dan dirotasi ketika digunakan. Setelah rotasi, token lama ditolak. Run E2E final memakai `ACCESS_TOKEN_TTL=60s`, menunggu token kedaluwarsa secara alami, lalu menguji refresh dan penolakan token lama.

## 7.2 Alasan keputusan dan batasan

Pemisahan identitas mencegah kredensial satu instansi atau client digunakan pada domain lain. Validasi scope di Client API memastikan pembatasan data berlaku di server, bukan hanya pada tampilan client. Penyimpanan token dalam bentuk hash mengurangi dampak jika data token tersimpan terbaca.

Auth Service menambah satu panggilan gRPC pada proses validasi akses, tetapi memberikan satu tempat untuk mengelola penerbitan, validasi, dan rotasi token. Milestone 1 belum mencakup integrasi dengan identity provider eksternal, revocation list terdistribusi, atau jalur pelaporan dari client ke BNPB.

## 7.3 Pengujian dan bukti penyelesaian

Kasus end to end `TestP3IndependentCredentialsAndRefreshRotation` menguji penolakan kredensial silang BMKG dan PVMBG, penolakan permintaan data mentah oleh Media, pemisahan data ringkasan dan mentah, kedaluwarsa access token Tim Lapangan, penerbitan token baru melalui refresh, serta penolakan refresh token lama.

Pengelolaan secret diperiksa melalui konfigurasi environment, file `.env.example`, dan audit source untuk memastikan nilai rahasia aktual tidak ditanam di source code. Hasil audit final dan screenshot demonstrasi perlu dilampirkan.

Hasil skenario autentikasi tercatat pada [artifact P3](../artifacts/p3-auth.json). Run E2E final memakai `ACCESS_TOKEN_TTL=60s` dan menunggu expiry secara alami sebelum menguji refresh serta penolakan token lama.

> **[PLACEHOLDER GAMBAR P3.1: Respons penolakan kredensial BMKG yang dipakai ke PVMBG dan sebaliknya.]**

> **[PLACEHOLDER GAMBAR P3.2: Respons Media untuk data ringkasan serta penolakan HTTP 403 saat meminta `raw=true`.]**

> **[PLACEHOLDER GAMBAR P3.3: Access token kedaluwarsa, refresh menghasilkan pasangan token baru, dan refresh token lama ditolak.]**

> **[PLACEHOLDER GAMBAR P3.4: File `.env.example` dan hasil audit bahwa secret aktual tidak berada di source code.]**

---

# 8. Problem 4 — Arsitektur microservice dan flexible storage

## 8.1 Rancangan solusi

### 8.1.1 Batas dan tanggung jawab service

BMKG dan PVMBG berjalan sebagai mock source independen dengan kontrak HTTP dan kredensial masing-masing. Aggregator bertanggung jawab atas polling, pemetaan, status sumber, Canonical Store, dan outbox. Auth Service mengelola identitas client dan token. Client API menyediakan endpoint downstream serta menegakkan otorisasi. Dashboard, Notifier, dan Portal berjalan sebagai consumer terpisah yang membaca event dari RabbitMQ.

Setiap service dijalankan dalam container tersendiri. Kode startup merangkai dependency service dan mengatur graceful shutdown; komunikasi antarkomponen tetap dilakukan melalui protokol jaringan yang ditetapkan.

### 8.1.2 Penyimpanan fleksibel dan kepemilikan data

Canonical Store menggunakan PostgreSQL. Field kanonik disimpan dalam kolom bertipe, sementara field khusus sumber dan field tambahan disimpan dalam `attributes JSONB`. Record yang dibuat sebelum dan sesudah `confidence_level` muncul dapat disimpan berdampingan tanpa perubahan tabel untuk setiap field baru.

Credential database Canonical Store hanya diberikan kepada Aggregator. Client API meminta data melalui gRPC Aggregator, Auth Service menggunakan penyimpanan autentikasinya sendiri, dan consumer hanya memerlukan akses ke RabbitMQ.

## 8.2 Alasan keputusan dan batasan

Service dipisah berdasarkan tanggung jawab dan kepemilikan data. Pemisahan ini memungkinkan satu komponen dibangun ulang atau dihentikan tanpa menggabungkan polling, autentikasi, penyajian API, dan pemrosesan consumer ke dalam satu unit deployment.

Penyimpanan dengan kolom tetap saja menjaga validasi dan query tetap sederhana, tetapi penambahan field dapat memerlukan migrasi. Document store menerima bentuk data dinamis secara alami, namun menambah teknologi dan pola query lain. PostgreSQL dengan JSONB dipilih karena field kanonik tetap terstruktur dan atribut tambahan dapat disimpan tanpa migrasi. Konsekuensinya, validasi dan query atas atribut dinamis tetap perlu ditangani aplikasi.

Canonical Store bukan database bersama untuk semua service. Penyimpanan fisik dan akses dapat dikelola dalam infrastruktur database yang sama, tetapi tanggung jawab akses terhadap data kanonik tetap berada pada Aggregator.

## 8.3 Pengujian dan bukti penyelesaian

Pengujian deployment menghentikan atau membangun ulang PVMBG secara terisolasi, lalu memeriksa bahwa container Aggregator dan Client API tidak ikut dibuat ulang serta permintaan yang tidak bergantung pada PVMBG tetap dapat dilayani. Hasil akhir E2E dan command yang digunakan perlu dicantumkan bersama bukti run.

Pemeriksaan storage memastikan data lama dan data baru dapat dibaca berdampingan, serta bahwa Client API tidak memiliki konfigurasi akses langsung ke Canonical Store. Manifest deployment juga digunakan untuk memperlihatkan pemisahan workload, penyimpanan persisten, secret reference, dan kebijakan jaringan.

Artifact [P4 independent rebuild](../artifacts/p4-independent-rebuild.json) mencatat bahwa identitas container Aggregator dan Client API tidak berubah saat PVMBG dibangun ulang, dan query BMKG sesudah rebuild menerima HTTP 200.

Artifact [P4 storage ownership](../artifacts/p4-storage-ownership.json) memeriksa konfigurasi container: Aggregator memiliki URL Canonical Store, sedangkan Client API dan Auth Service tidak menerima URL tersebut.

> **[PLACEHOLDER GAMBAR P4.1: Daftar container sebelum dan sesudah PVMBG dibangun ulang, dengan identitas container service lain tetap.]**

> **[PLACEHOLDER GAMBAR P4.2: Record HazardEvent sebelum dan sesudah `confidence_level` muncul dalam JSONB.]**

> **[PLACEHOLDER GAMBAR P4.3: Manifest atau konfigurasi service yang menunjukkan hanya Aggregator memiliki akses ke Canonical Store.]**

---

# 9. Problem 5 — Fan-out event distribution / pub-sub

## 9.1 Rancangan solusi

### 9.1.1 Producer, broker, dan consumer

Setelah HazardEvent disimpan, Aggregator mencatat pesan outbox dalam transaksi yang sama. Outbox worker mengambil pesan tertunda melalui mekanisme lease dan menerbitkannya sebagai Protobuf ke exchange fanout `hazard.events` pada RabbitMQ. Publisher menggunakan konfirmasi broker dan delivery persisten. RabbitMQ menyediakan durable queue terpisah untuk Dashboard, Notifier, dan Portal, sehingga setiap consumer menerima salinan event tanpa producer memanggilnya secara langsung.

Consumer menggunakan manual acknowledgement setelah pemrosesan berhasil. Payload yang gagal diproses ditolak tanpa requeue dan diarahkan melalui dead letter exchange ke queue dead letter milik consumer. Jika koneksi broker terputus, consumer memulai ulang proses konsumsi setelah channel ditutup.

### 9.1.2 Semantik pengiriman dan idempotensi

Pola outbox dan publisher confirmation memberikan semantik at least once. Akibatnya, event dapat terkirim ulang apabila konfirmasi atau acknowledgement tidak diterima. Consumer mengenali event yang sudah diproses menggunakan `hazard_id`. Deduplikasi saat ini disimpan di memori selama proses consumer berjalan, sehingga pesan yang dikirim ulang setelah consumer dimulai ulang masih dapat diproses kembali.

Test `TestProcessorDeduplicatesWithinProcessButNotAcrossRestart` tersedia pada Dashboard, Notifier, dan Portal consumer. Test memastikan redelivery dengan `hazard_id` yang sama tidak menghasilkan pemrosesan kedua pada processor yang sama, serta mendokumentasikan bahwa processor baru tidak membawa cache deduplikasi lama.

## 9.2 Alasan keputusan dan batasan

RabbitMQ dipilih karena exchange fanout dan durable queue menyediakan pemisahan distribusi per consumer dengan konfigurasi yang sesuai untuk demonstrasi lokal. Kafka juga dapat menyediakan aliran event dan replay, tetapi pengoperasiannya lebih berat untuk kebutuhan fanout M1.

At least once mengutamakan pengiriman ulang ketika hasil publikasi atau pemrosesan belum dapat dipastikan. Konsekuensinya, consumer perlu menangani duplikasi. Deduplikasi yang digunakan saat ini tidak persisten melewati restart consumer. Deployment RabbitMQ lokal juga single node dan bukan konfigurasi high availability produksi.

## 9.3 Pengujian dan bukti penyelesaian

Pengujian fanout menghentikan Notifier sementara Aggregator dan Dashboard tetap berjalan. Selama Notifier berhenti, event terus diterbitkan dan backlog pada queue Notifier bertambah. Setelah Notifier dimulai kembali, queue diproses hingga backlog berkurang. Portal dapat ditambahkan sebagai consumer baru tanpa mengubah producer.

Queue dead letter yang digunakan adalah `dashboard.hazard-events.dead-letter`, `notifier.hazard-events.dead-letter`, dan `portal.hazard-events.dead-letter`. Bukti demonstrasi perlu mencakup queue, jumlah consumer dan pesan, pemulihan backlog, serta log pemrosesan dengan correlation ID.

Hasil run tercatat pada [artifact fanout](../artifacts/p5-fanout.json), [artifact consumer baru](../artifacts/p5-new-consumer.json), dan [artifact pemulihan outbox](../artifacts/p5-outbox-recovery.json). Run ini mencatat 10 pesan outbox tertunda saat broker berhenti dan 0 pesan tertunda setelah broker pulih.

> **[PLACEHOLDER GAMBAR P5.1: Daftar exchange, durable queue, dead letter queue, jumlah consumer, dan jumlah pesan.]**

> **[PLACEHOLDER GAMBAR P5.2: Notifier dihentikan sementara Dashboard tetap berjalan dan backlog Notifier bertambah.]**

> **[PLACEHOLDER GAMBAR P5.3: Backlog setelah Notifier aktif kembali serta Portal ditambahkan tanpa perubahan producer.]**

> **[PLACEHOLDER GAMBAR P5.4: Log event consumer yang memuat `hazard_id`, correlation ID, dan status redelivery.]**

---

# 10. Observability, security, dan operasi

Observability dan konfigurasi operasional mendukung pemeriksaan kondisi service serta penelusuran alur request antarservice. Rincian ini membantu menghubungkan hasil pengujian dengan log dan konfigurasi yang digunakan.

## 10.1 Observability service

### 10.1.1 Health check dan status sumber

Service menyediakan health check untuk pemantauan melalui Docker Compose maupun Kubernetes. Aggregator juga menyediakan status terpisah untuk BMKG dan PVMBG, termasuk status `healthy` atau `stale` dan informasi `stale_since` ketika polling sumber gagal.

### 10.1.2 Structured logging dan correlation ID

Log service ditulis dalam format terstruktur dan memuat informasi seperti nama service, operasi, hasil, serta correlation ID. Correlation ID digunakan untuk menghubungkan request yang melintasi Client API, Auth Service, Aggregator, mock source, dan consumer. Client API membatasi request bersamaan melalui `MAX_INFLIGHT_REQUESTS`, dengan nilai default 64, serta durasi request melalui `REQUEST_TIMEOUT`, dengan nilai default 5 detik.

## 10.2 Keamanan dan konfigurasi

Kredensial dan konfigurasi runtime dibaca dari environment. File `.env.example` memuat nama variabel dan nilai placeholder. Secret yang sebenarnya harus disediakan melalui file lokal yang tidak di-commit atau melalui Kubernetes Secret. Image service dikonfigurasi untuk menjalankan proses sebagai user non-root.

## 10.3 Petunjuk menjalankan sistem

### 10.3.1 Menjalankan stack lokal

Dari kondisi awal, salin file contoh environment, generate kontrak Protobuf, lalu jalankan stack dengan Docker Compose:

```sh
cp .env.example .env
make generate
docker compose --env-file .env up --build
```

### 10.3.2 Pemeriksaan dan pengujian

Setelah service tersedia, jalankan health check dan target pengujian berikut:

```sh
curl http://localhost:8080/health
curl http://localhost:8090/health
make test
make test-contract
make test-e2e
```

### 10.3.3 Deployment K3d lokal

Cluster K3d dan skala Client API dapat disiapkan menggunakan target berikut:

```sh
K3S_ENV_FILE=.env K3D_CLUSTER=bnpb-m1 K3D_HOST_PORT=18080 make k3d-deploy
K3S_NAMESPACE=bnpb-m1 K3D_SCALE_SERVICE=client-api K3D_REPLICAS=3 make k3d-scale
```

README repository memuat daftar environment variable, langkah menjalankan sistem dari kondisi awal, dan cara memicu skenario demo. Nilai placeholder harus diganti dengan secret lokal sebelum deployment.

---

# 11. Ringkasan pengujian dan bukti

Bagian ini merangkum hasil pengujian yang tercatat dan menghubungkannya dengan kriteria spesifikasi. Hasil harus dicocokkan dengan artifact dari run final sebelum laporan diekspor menjadi PDF.

## 11.1 Ringkasan hasil pengujian

| Pengujian | Perintah atau artifact | Hasil run final |
|---|---|---|
| Go unit test seluruh module | `GOFLAGS=-count=1 make test` | Lulus seluruh package |
| Contract test | `GOFLAGS=-count=1 make test-contract` | Lulus |
| Integration test | `GOFLAGS=-count=1 make test-integration` | Lulus terhadap stack lokal aktif |
| Frontend unit test | `npm test -- --run` | 4 file, 14 test lulus |
| Frontend production build | `npm run build` | TypeScript dan Vite build lulus |
| Playwright console smoke | `make test-demo-e2e` dengan `DEMO_CONSOLE_URL=http://127.0.0.1:3001` | 2 test lulus terhadap source pada Vite dev server |
| E2E P1 sampai P5 dengan TTL default | `E2E_ACCESS_TOKEN_TTL=60s make test-e2e` | Lulus; mencakup expiry alami setelah 60 detik dan artifact P1 sampai P5 |
| P2 isolasi dengan PVMBG delay maksimum 3 detik | [Artifact isolasi](../artifacts/p2-isolation.json) | 25 VU per source; BMKG p50 3,445 ms, p95 8,790 ms, p99 19,462 ms; error 0% |
| P2 sustained load | [Artifact sustained](../artifacts/p2-sustained.json) | 50 VU selama 60 detik; 28.211 request; 468,899 request per detik; p50 4,888 ms; p95 13,970 ms; p99 19,915 ms; error 0% |
| P5 backlog dan outbox recovery | [Artifact fanout](../artifacts/p5-fanout.json), [artifact outbox](../artifacts/p5-outbox-recovery.json) | Dashboard tetap menerima event; Notifier mengejar backlog; 10 outbox tertunda saat broker mati dan 0 setelah pulih |
| P5 duplicate delivery | `TestProcessorDeduplicatesWithinProcessButNotAcrossRestart` pada tiga consumer | Satu pemrosesan per `hazard_id` dalam process yang sama; cache deduplikasi tidak bertahan setelah restart |

## 11.2 Keterlacakan kriteria dan bukti

| Kriteria | Langkah pembuktian | Bukti repository/run | Status laporan |
|---|---|---|---|
| P1.1 Mapping kedua sumber | Validasi field kanonik, severity, koordinat, atribut sumber, dan korelasi warning | `TestP1CanonicalMappingForBothSources`, `TestSeismicSeverityMapping`, `TestWarningsByEventIDCorrelatesUsingRelatedEventID`, `artifacts/p1-canonical-mapping.json` | Lulus; 90 event per sumber |
| P1.2 Schema change tanpa restart | POST `/admin/schema-version` saat stack berjalan | `TestP1SchemaEvolutionKeepsPVMBGHazardsValid` | Lulus pada E2E |
| P1.3 Aggregator tetap valid | Query raw sesudah schema toggle | `artifacts/p1-schema-evolution.json` | Lulus; 25 record lama dan 64 record baru pada run final |
| P1.4 Kebijakan unknown field | Bandingkan attributes sebelum dan sesudah | Dynamic mapping dan JSONB | Lulus; field baru diteruskan ke attributes |
| P1.5 Mekanisme dan batasan | Jelaskan additive only dan perubahan yang tidak didukung | Bab 5 serta test pemetaan | Dicakup pada laporan |
| P2.1 BMKG only p95 di bawah 300 ms saat PVMBG delay 3 detik | Jalankan 25 VU BMKG dan 25 VU PVMBG bersamaan | [Artifact isolasi](../artifacts/p2-isolation.json) | Lulus; BMKG p95 8,790 ms, error 0% |
| P2.2 Sustained load minimal 50 VU selama 60 detik | Jalankan k6 dan catat percentile lengkap | [Artifact sustained](../artifacts/p2-sustained.json) | Lulus; 28.211 request, p50 4,888 ms, p95 13,970 ms, p99 19,915 ms, error 0% |
| P2.3 PVMBG outage dan recovery | Toggle outage, cek stale, akses kedua sumber, pulihkan | `TestP2PVMBGOutageKeepsBMKGAvailable`, `artifacts/p2-outage.json` | Lulus pada E2E |
| P2.4 Model concurrency | Jelaskan poll cycle, timeout, dan batas inflight | `services/aggregator/internal/application/ingest`, config Client API | Dicakup pada Bab 6 |
| P3.1 Credential isolation | Gunakan credential silang ke mock | `TestP3IndependentCredentialsAndRefreshRotation`, `artifacts/p3-auth.json` | Lulus pada E2E |
| P3.2 Media tidak dapat meminta data mentah | Request Media dengan `raw=true` dan periksa proyeksi | E2E API serta Playwright smoke | HTTP 403 dan field mentah tidak bocor |
| P3.3 Expiry dan refresh token | Tunggu TTL 60 detik, refresh, coba token lama | E2E final dan `artifacts/p3-auth.json` | Lulus setelah expiry alami 60 detik |
| P3.4 Secret hygiene | Periksa file environment tracked dan sumber konfigurasi credential | `git ls-files '.env' '.env.*'`, konfigurasi service | `.env.example` satu satunya file environment tracked; credential dibaca melalui `os.Getenv` |
| P3.5 Mekanisme trust | Jelaskan interceptor, scope, dan hash token | Auth adapter, ports, transport | Dicakup pada Bab 7 |
| P4.1 Restart container independen | Rebuild PVMBG dan periksa container lain | `artifacts/p4-independent-rebuild.json` | Lulus; Aggregator dan Client API tidak dibuat ulang |
| P4.2 Flexible storage | Baca record sebelum dan sesudah schema change | `artifacts/p4-jsonb-coexistence.json` | Lulus tanpa migrasi field baru |
| P4.3 Kepemilikan storage | Periksa environment container | `artifacts/p4-storage-ownership.json` | Lulus; URL Canonical Store hanya ada di Aggregator |
| P4.4 Batas service dan alternatif storage | Bandingkan PostgreSQL JSONB dan document store | Bab 8 | Dicakup pada laporan |
| P5.1 Publish event ke broker | Periksa outbox, exchange, dan queue | RabbitMQ adapter, outbox worker | Dicakup pada Bab 9 dan artifact |
| P5.2 Consumer independen | Hentikan Notifier dan pantau Dashboard | `artifacts/p5-fanout.json` | Lulus; Dashboard tetap memproses |
| P5.3 Pemulihan backlog | Nyalakan Notifier dan tunggu queue kosong | `artifacts/p5-fanout.json` | Lulus pada E2E |
| P5.4 Tambah consumer tanpa mengubah producer | Aktifkan Portal consumer | `artifacts/p5-new-consumer.json` | Lulus pada E2E |
| P5.5 Semantik dan idempotensi | Uji duplikasi dalam proses dan perilaku setelah restart | `TestProcessorDeduplicatesWithinProcessButNotAcrossRestart` pada tiga consumer | Deduplikasi per proses lulus; tidak persisten setelah restart |

## 11.3 Bukti yang masih perlu dilengkapi

Pengujian P2.1 dan P2.2 kini menghasilkan artifact k6 lengkap dengan p50, p95, p99, throughput, error rate, dan controlled rejection. Pengujian E2E final juga memakai TTL token 60 detik, mencakup mapping awal kedua sumber, memeriksa kepemilikan Canonical Store, dan membuktikan backlog outbox pulih setelah broker kembali.

Batasan yang masih ada adalah deduplikasi consumer yang hanya berlaku selama process hidup. Test membuktikan perilaku ini secara eksplisit; delivery ulang setelah restart dapat diproses kembali. Screenshot demonstrasi, identitas, dan tabel kontribusi anggota tetap perlu dilengkapi oleh kelompok. Playwright smoke test dijalankan terhadap source pada Vite dev server port 3001; container Demo Console di port 3000 belum dibangun ulang setelah perbaikan handler karena unduhan base image Node berjalan sangat lambat.

---

# 12. Pernyataan penggunaan LLM

LLM digunakan sebagai alat bantu untuk mengeksplorasi repository, menyusun rencana refactor, menambah dan memperbaiki test, menganalisis hasil pengujian, meninjau struktur, serta menyusun draft dokumentasi. Penggunaan tersebut tidak menggantikan tanggung jawab kelompok untuk memahami hasilnya.

Anggota kelompok perlu meninjau kode dan konfigurasi yang dihasilkan atau diubah dengan bantuan LLM, memastikan kesesuaiannya dengan spesifikasi, dan menjalankan pengujian yang relevan. Klaim dan bukti pada laporan final harus diverifikasi oleh kelompok menggunakan environment demonstrasi yang digunakan.

---

# 13. Kesimpulan

Sistem Milestone 1 dirancang sebagai kumpulan service yang berkomunikasi melalui REST, gRPC, dan RabbitMQ. Aggregator mengambil data dari BMKG dan PVMBG melalui polling, memetakannya ke HazardEvent kanonik, dan menyimpan atribut dinamis dalam JSONB. Client API mengatur akses downstream, Auth Service mengelola identitas dan token, sedangkan RabbitMQ memisahkan producer dari consumer melalui queue independen.

Pengujian yang dijalankan menghasilkan bukti untuk pemetaan data kedua sumber, perubahan skema saat berjalan, degradasi saat outage, expiry token default, isolasi deployment, kepemilikan Canonical Store, serta fanout dan pemulihan outbox. Load test memenuhi p95 BMKG only di bawah 300 milidetik saat PVMBG dikonfigurasi hingga 3 detik dan error rate di bawah 1 persen pada sustained run. Deduplikasi consumer telah diuji dalam satu process, tetapi belum disimpan secara durable melewati restart. Screenshot demonstrasi, identitas anggota, dan tabel kontribusi tetap perlu dilengkapi sebelum PDF dikumpulkan.
