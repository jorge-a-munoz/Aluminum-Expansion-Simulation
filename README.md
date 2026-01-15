# IIoT Digital Twin: CMM Thermal Compensation

> A full-stack Industrial IoT prototype demonstrating **Edge Computing**, **Real-Time Physics Simulation**, and **Network Resilience** (Store-and-Forward).

---

## Project Overview
In precision manufacturing, temperature fluctuations cause materials to expand, leading to false quality failures. This project simulates a **Coordinate Measuring Machine (CMM)** inspecting an Aluminum 6061 part. 

It implements a "Digital Twin" architecture where an Edge Gateway captures raw sensor data, compensates for thermal expansion in real-time, and fans out telemetry to multiple industrial protocols (OPC-UA, HTTP/MES, and InfluxDB).

### Architecture
**Data Flow:** `Sensor -> File System -> Edge Gateway -> [OPC-UA | MES Cloud | Historian]`

| Component | Technology | Responsibility |
| :--- | :--- | :--- |
| **Physics Engine** | Python, NumPy | Simulates CTE (Coeff. of Thermal Expansion) and Sensor Noise. |
| **Edge Gateway** | Python `asyncio` | Non-blocking file watcher, data ingestion, and protocol bridging. |
| **Middleware** | OPC-UA (AsyncUA) | Exposes live tags to local SCADA/PLC systems. |
| **Resilience** | Python `deque` | Implements "Store-and-Forward" buffering during network outages. |
| **Historian** | InfluxDB v2 | High-speed time-series storage for analytics. |
| **Visualization** | Grafana | Real-time dashboard for thermal correlation & infrastructure health. |

---

## Quick Start Guide

### 1. Infrastructure (Docker)
Start the Time-Series Database and Dashboard container.
```bash
docker compose up -d
```
*   **InfluxDB:** `http://localhost:8086` (User: `sanmina_admin` / Pass: `SanminaSecurePass123!`)
*   **Grafana:** `http://localhost:3000` (User: `admin` / Pass: `admin`)

### 2. Python Environment
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Run the System (The "Three Terminal" Setup)
You need three separate terminal windows to simulate the distributed system:

**Terminal 1: The Cloud (Mock MES)**
*Starts the API server that receives quality data.*
```bash
python mock_mes.py
```

**Terminal 2: The Hardware (Simulator)**
*Generates CSV sensor data with thermal drift.*
```bash
python cmm_simulator.py
```

**Terminal 3: The Edge Gateway**
*Watches files, processes physics, and transmits data.*
```bash
python cmm_edge_gateway.py
```

---

## The Dashboard
The project includes a pre-configured Grafana Dashboard.
1.  Log into Grafana (`localhost:3000`).
2.  Go to **Dashboards -> Import**.
3.  Upload the `cmm_dashboard.json` file included in this repo.
4.  **Visuals:**
    *   **Thermal Correlation:** Observe the Red Line (Temp) and Blue Line (Length) moving in sync.
    *   **Network Buffer:** Shows the internal queue depth of the Edge Gateway.

---

## Chaos Engineering (How to Test)
This system is built to survive network failures.

1.  **Simulate Outage:** While the system is running, go to **Terminal 1** (Mock MES) and press `Ctrl+C` to kill the server.
2.  **Verify Resilience:**
    *   Check **Terminal 3**: Logs will show `WARNING: Network Lost! Buffering...`
    *   Check **Grafana**: The "Buffer Depth" graph will spike upwards.
3.  **Restore Network:** Restart `python mock_mes.py` in Terminal 1.
4.  **Verify Recovery:**
    *   Check **Terminal 3**: Logs will show `INFO: Flushing buffer...`
    *   Check **Grafana**: The buffer graph drops instantly to zero. No data is lost in InfluxDB.

---

## Design Decisions (Interview Talking Points)

### Why `asyncio` instead of Threading?
I/O operations (Database writes, HTTP requests, OPC updates) are slow. Using Python's `async/await` allows the Gateway to handle high-frequency sensor data without blocking the main execution loop, ensuring the CMM never waits on the Cloud.

### Why InfluxDB & Flux?
Standard SQL databases struggle with high-frequency timestamped data. InfluxDB allows for efficient querying of time windows (e.g., "Last 5 minutes") and the Flux query language enables server-side data aggregation (Windowing/Downsampling) before visualization.

### Docker Strategy
The database and dashboard are decoupled from the application logic. This allows the Python Edge Gateway to run on a lightweight Industrial PC (IPC) or Raspberry Pi, while the heavy storage/viz runs on a centralized server or cloud instance.