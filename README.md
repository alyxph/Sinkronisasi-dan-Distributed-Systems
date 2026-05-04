# Distributed Synchronization System

### Authors
- **Nama:** Alief Rachmattul Islam
- **NIM:** 11231007
- **Mata Kuliah:** Sistem Paralel dan Terdistribusi
- **Kelas:** A

---

Sistem sinkronisasi terdistribusi yang menyediakan **Distributed Lock Manager**, **Message Queue**, dan **Distributed Cache** secara murni (100% Full Lokal) tanpa database eksternal. Sistem ini menggunakan in-memory state dan persistence via Write-Ahead Log (WAL) untuk menjamin ketahanan data.

---

**Distributed Synchronization System** dirancang untuk mengelola sinkronisasi data dan koordinasi antar node dalam sistem terdistribusi. Dibangun secara mandiri (*from scratch*), sistem ini memastikan konsistensi data, high availability, dan fault tolerance melalui implementasi algoritma konsensus dan P2P.

1. **Distributed Lock Manager**
   - Manajemen lock (shared/exclusive) untuk akses resource
   - Konsensus menggunakan algoritma Raft (Leader Election & Log Replication)
   - Deteksi deadlock menggunakan DFS (wait-for graph) otomatis
   - Automatic failover transition antar node

2. **Distributed Queue System**
   - Message queue terdistribusi dengan *consistent hashing* (virtual nodes)
   - Persistent storage murni menggunakan file lokal (Write-Ahead Log / WAL)
   - Jaminan *At-least-once delivery*
   - Automatic message recovery pasca restart node

3. **Distributed Cache System**
   - Protokol cache coherence MESI (Modified, Exclusive, Shared, Invalid)
   - Kebijakan eviksi memori (LRU/LFU)
   - Invalidation broadcast murni secara P2P HTTP
   - Konsistensi tinggi dengan local read speed

---

### Struktur Direktori
```text
TUGAS3/
├── README.md
├── requirements.txt
├── run_all_local.ps1
├── benchmarks/
│   └── load_test_scenarios.py
├── docker/
│   ├── docker-compose.yml
│   ├── Dockerfile.node
│   ├── nginx-cache.conf
│   ├── nginx-lock.conf
│   └── nginx-queue.conf
├── docs/
│   ├── api_spec.yaml
│   ├── architecture.md
│   └── deployment_guide.md
├── scratch/
│   └── check_status.py
├── src/
│   ├── __init__.py
│   ├── main.py
│   ├── communication/
│   │   ├── __init__.py
│   │   ├── failure_detector.py
│   │   └── message_passing.py
│   ├── consensus/
│   │   ├── __init__.py
│   │   ├── pbft.py
│   │   └── raft.py
│   ├── nodes/
│   │   ├── __init__.py
│   │   ├── base_node.py
│   │   ├── cache_node.py
│   │   ├── lock_manager.py
│   │   └── queue_node.py
│   ├── swagger/
│   │   ├── __init__.py
│   │   ├── openapi.yaml
│   │   └── swagger_ui.py
│   └── utils/
│       ├── __init__.py
│       ├── config.py
│       ├── hashing.py
│       └── metrics.py
└── tests/
  ├── __init__.py
  ├── smoke_test_docker.py
  ├── integration/
  │   ├── __init__.py
  │   └── test_lock_api.py
  ├── performance/
  │   ├── __init__.py
  │   └── test_load_basics.py
  └── unit/
    ├── __init__.py
    ├── test_consistent_hash.py
    └── test_lock_state.py
```

---

### High-Level Architecture
```text
┌─────────────────────────────────────────────────────────┐
│                   CLIENT APPLICATIONS                   │
└────────────┬────────────────┬────────────────┬──────────┘
             │                │                │
             ▼                ▼                ▼
    ┌────────────────┐ ┌────────────┐ ┌────────────────┐
    │ Lock Manager   │ │   Queue    │ │     Cache      │
    │  (Port 7001+)  │ │(Port 7101+)│ │  (Port 7201+)  │
    └────────┬───────┘ └──────┬─────┘ └────────┬───────┘
             │                │                 │
             └────────────────┼─────────────────┘
                              │
             ┌────────────────▼─────────────────┐
             │    DISTRIBUTED PEER-TO-PEER      │
             │                                  │
             │                                  │
             │  ┌────────┐  ┌────────┐  ┌────────┐
             │  │ Node 1 │◄─┤ Node 2 │◄─┤ Node 3 │
             │  └────────┘  └────────┘  └────────┘
             │                                  │
             │       Raft Consensus & HTTP      │
             └──────────────────────────────────┘
```

### Teknologi Stack
- **Backend Framework**: `aiohttp` (Python 3.11+)
- **Consensus Algorithm**: Raft
- **Cache Protocol**: MESI Coherence
- **Hashing**: Consistent Hashing (MD5 ring)
- **Persistence**: File-based `.wal` (Write-Ahead Log)
- **Communication**: P2P HTTP/REST (Internal RPC)
- **Containerization**: Docker + Docker Compose

---

### Prerequisites
Pastikan Anda sudah menginstall:
- **Python 3.11+**
- **Docker** dan **Docker Compose**
- **Git**

---

### 1. Clone Repository
```bash
git clone https://github.com/alyxph/Sinkronisasi-dan-Distributed-Systems.git
```

---

### 2. Deployment dengan Docker Compose (Recommended)
Sistem dirancang mandiri tanpa butuh third-party DB. Anda bisa mendeploy ke-9 node (3 Lock, 3 Queue, 3 Cache) secara instan.

#### a. Setup Configuration
```bash
cp .env.example .env
```

#### b. Build & Start All Services
```bash
docker compose -f docker/docker-compose.yml up --build -d
```

Ini akan menjalankan:
- **3 Lock Manager nodes** (ports 7001, 7002, 7003)
- **3 Queue nodes** (ports 7101, 7102, 7103)
- **3 Cache nodes** (ports 7201, 7202, 7203)

#### c. Verify Services
```bash
# Check running containers
docker compose -f docker/docker-compose.yml ps

# Test health endpoints
curl http://localhost:7001/health  # Lock Manager
curl http://localhost:7101/health  # Queue
curl http://localhost:7201/health  # Cache
```

#### d. Check Raft Status
```bash
curl http://localhost:7001/status
```

---

### 3. Manual Installation (Development Mode)
#### a. Setup Python Environment

```bash
# Windows
python -m venv venv
.\venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

#### b. Run Nodes Manually
Jalankan node dari root direktori (ubah `NODE_ROLE` sesuai kebutuhan: `lock_manager`, `queue_node`, `cache_node`):

```bash
export NODE_ROLE=lock_manager
export NODE_ID=lock-1
export PORT=7001
export PEERS='[{"id":"lock-1","url":"http://localhost:7001"},{"id":"lock-2","url":"http://localhost:7002"}]'
python -m src.main
```

---

### 🚀 Usage Examples

#### Lock Manager
**Acquire Lock**
```bash
curl -X POST http://localhost:7001/lock/acquire \
  -H "Content-Type: application/json" \
  -d '{
    "resource": "database:users",
    "client_id": "service_a",
    "lock_type": "exclusive"
  }'
```

**Release Lock**
```bash
curl -X POST http://localhost:7001/lock/release \
  -H "Content-Type: application/json" \
  -d '{
    "resource": "database:users",
    "client_id": "service_a"
  }'
```

#### Queue System
**Publish Message**
```bash
curl -X POST http://localhost:7101/queue/publish \
  -H "Content-Type: application/json" \
  -d '{
    "queue": "orders",
    "message": {"order_id": 123, "item": "Laptop"}
  }'
```

**Consume Message**
```bash
curl -X POST http://localhost:7101/queue/consume \
  -H "Content-Type: application/json" \
  -d '{"queue": "orders"}'
```

**Acknowledge Message**
```bash
curl -X POST http://localhost:7101/queue/ack \
  -H "Content-Type: application/json" \
  -d '{"queue": "orders", "message_id": "<MSG_ID_DARI_CONSUME>"}'
```

#### Cache System
**Write to Cache (Otomatis broadcast P2P)**
```bash
curl -X POST http://localhost:7201/cache/put \
  -H "Content-Type: application/json" \
  -d '{
    "key": "user:123",
    "value": {"name": "Alice", "age": 30}
  }'
```

**Read from Cache**
```bash
curl http://localhost:7201/cache/get?key=user:123
```

---

### 🧪 Tests & Benchmarks

**Unit & Integration Testing (Pytest)**
```bash
pip install pytest pytest-asyncio
pytest tests/ -v
```

**Load Testing (Locust)**
```bash
pip install locust
locust -f benchmarks/load_test_scenarios.py --web-host=localhost
# Buka http://localhost:8089 di browser
```

**Interactive Demo via Swagger UI**
```
# Buka salah satu URL berikut di browser setelah klaster berjalan:
http://localhost:7001/docs   # Swagger UI — Lock Manager
http://localhost:7101/docs   # Swagger UI — Queue Node
http://localhost:7201/docs   # Swagger UI — Cache Node
```

---

### 📖 Dokumentasi Lengkap
- **[Architecture Documentation](docs/architecture.md)** - Detail arsitektur sistem full-lokal, P2P WAL, dan Raft.
- **[API Specification](docs/api_spec.yaml)** - OpenAPI specification (Swagger) untuk integrasi endpoints.
- **[Deployment Guide](docs/deployment_guide.md)** - Panduan Docker Compose dan environment var.

---

### 📝 Git Commit History
- **first commit** - Initial project setup dengan implementasi lengkap Distributed Lock Manager, Queue System, Cache System, dan Raft consensus.

