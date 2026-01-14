import asyncio
import logging
import os
import time
import datetime
import pandas as pd
from typing import Optional
from collections import deque

import aiohttp 
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from asyncua import Server, ua

# --- NEW IMPORTS FOR INFLUXDB ---
from influxdb_client import Point
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync

# --- Configuration ---
class Config:
    # OPC-UA Settings
    OPC_ENDPOINT = "opc.tcp://0.0.0.0:4840/freeopcua/server/"
    OPC_NAMESPACE_URI = "http://sanmina.com/cmm/digital_twin"
    
    # File System
    WATCH_DIR = "cmm_data_output"
    MAX_RETRIES = 5
    RETRY_DELAY_SECONDS = 0.5

    # Edge Analytics
    MAX_TEMP_THRESHOLD_C = 24.0

    # IT / Cloud Settings
    MES_API_URL = "http://127.0.0.1:8000/api/v1/cmm/measurements"
    MACHINE_ID = "SANMINA_CMM_01"
    MAX_BUFFER_SIZE = 1000 

    # --- INFLUXDB SETTINGS (Matches docker-compose & .env) ---
    INFLUX_URL = "http://localhost:8086"
    INFLUX_TOKEN = "my-super-secret-admin-token-for-dev"
    INFLUX_ORG = "sanmina_manufacturing"
    INFLUX_BUCKET = "cmm_telemetry"

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("EdgeGateway")


class InfluxConnector:
    """
    Handles asynchronous writes to the Time Series Database.
    This provides the 'Historian' capability.
    """
    def __init__(self):
        self.client = InfluxDBClientAsync(
            url=Config.INFLUX_URL,
            token=Config.INFLUX_TOKEN,
            org=Config.INFLUX_ORG
        )
        self.write_api = self.client.write_api()

    async def write_telemetry(self, temp: float, length: float, buffer_size: int, timestamp: datetime.datetime):
        """
        Writes a single data point to InfluxDB.
        """
        try:
            # Create a "Point" - the atomic unit of InfluxDB data
            point = (
                Point("cmm_metrology")
                .tag("machine_id", Config.MACHINE_ID)
                .field("temperature_c", temp)
                .field("measured_length_mm", length)
                .field("buffer_queue_size", int(buffer_size)) # Operational Metric
                .time(timestamp)
            )

            await self.write_api.write(bucket=Config.INFLUX_BUCKET, record=point)
            logger.debug(f"InfluxDB Write Success: {length}mm")

        except Exception as e:
            # We log error but DO NOT raise it. 
            # Failure to write history should not stop production MES reporting.
            logger.error(f"InfluxDB Write Failed: {e}")

    async def close(self):
        """Cleanly close the async client"""
        await self.client.close()


class CloudConnector:
    """
    Handles HTTP communication with the MES.
    Implements 'Store and Forward' logic.
    """
    def __init__(self):
        self._buffer = deque(maxlen=Config.MAX_BUFFER_SIZE)
        self.session = None 

    async def init_session(self):
        self.session = aiohttp.ClientSession()

    async def close_session(self):
        if self.session:
            await self.session.close()

    def get_buffer_size(self) -> int:
        """Expose buffer size for telemetry monitoring"""
        return len(self._buffer)

    async def send_measurement(self, payload: dict):
        sent = await self._post_to_cloud(payload)

        if not sent:
            self._buffer.append(payload)
            if len(self._buffer) % 10 == 0: # Log only occasionally to reduce noise
                logger.warning(f"Network Connection Lost! Buffering. Size: {len(self._buffer)}")
        else:
            if len(self._buffer) > 0:
                logger.info(f"Network Restored! Flushing {len(self._buffer)} items...")
                await self._flush_buffer()

    async def _post_to_cloud(self, payload: dict) -> bool:
        if self.session is None:
            await self.init_session()
        try:
            async with self.session.post(Config.MES_API_URL, json=payload) as response:
                return response.status == 200
        except Exception:
            return False

    async def _flush_buffer(self):
        while len(self._buffer) > 0:
            item = self._buffer[0]
            success = await self._post_to_cloud(item)
            if success:
                self._buffer.popleft()
            else:
                break


class CmmOpcServer:
    def __init__(self):
        self.server = Server()
        self.server.set_endpoint(Config.OPC_ENDPOINT)
        self.server.set_server_name("Sanmina_CMM_Gateway")
        self.idx = 0
        self.var_temp = None; self.var_length = None; self.var_status = None; self.var_last_update = None

    async def init(self):
        await self.server.init()
        self.idx = await self.server.register_namespace(Config.OPC_NAMESPACE_URI)
        objects = self.server.nodes.objects
        cmm_obj = await objects.add_object(self.idx, "Sanmina_CMM_01")
        self.var_temp = await cmm_obj.add_variable(self.idx, "Temperature_C", 0.0)
        self.var_length = await cmm_obj.add_variable(self.idx, "Measured_Length_mm", 0.0)
        self.var_status = await cmm_obj.add_variable(self.idx, "Device_Status", "IDLE")
        self.var_last_update = await cmm_obj.add_variable(self.idx, "Last_Update", "N/A")

    async def start(self): await self.server.start()
    async def stop(self): await self.server.stop()

    async def update_values(self, temp, length, timestamp, status):
        try:
            await self.var_temp.write_value(temp)
            await self.var_length.write_value(length)
            await self.var_last_update.write_value(timestamp)
            await self.var_status.write_value(status)
            logger.info(f"Processing: [{status}] Temp={temp:.2f}C | Len={length:.4f}")
        except Exception as e:
            logger.error(f"OPC Error: {e}")


class FileProcessor:
    def __init__(self, opc_server: CmmOpcServer, cloud_connector: CloudConnector, influx_connector: InfluxConnector):
        self.opc_server = opc_server
        self.cloud_connector = cloud_connector
        self.influx_connector = influx_connector # Inject Influx Dependency

    async def process_file(self, file_path: str):
        df = await self._robust_read_csv(file_path)
        
        if df is not None and not df.empty:
            try:
                latest = df.iloc[-1]
                temp = float(latest['Simulated_Temperature_C'])
                length = float(latest['Observed_Part_Length_mm'])
                
                # Timestamp handling: Convert string to datetime object for Influx
                timestamp_str = str(latest['Timestamp'])
                timestamp_dt = pd.to_datetime(timestamp_str).to_pydatetime()

                status = "FAIL" if temp > Config.MAX_TEMP_THRESHOLD_C else "ACTIVE"

                # --- PARALLEL EXECUTION ---
                # We launch three tasks simultaneously using asyncio.gather.
                # 1. Update PLC/SCADA (OPC-UA)
                task_opc = self.opc_server.update_values(temp, length, timestamp_str, status)
                
                # 2. Update MES (Cloud JSON)
                json_payload = {
                    "machine_id": Config.MACHINE_ID,
                    "timestamp": timestamp_str,
                    "temperature": temp,
                    "length": length,
                    "status": status
                }
                task_cloud = self.cloud_connector.send_measurement(json_payload)

                # 3. Update Historian (InfluxDB)
                # Note: We grab current buffer size to track network health in Grafana
                current_buffer = self.cloud_connector.get_buffer_size()
                task_influx = self.influx_connector.write_telemetry(temp, length, current_buffer, timestamp_dt)

                # Await all. return_exceptions=True ensures one failure doesn't crash the others.
                await asyncio.gather(task_opc, task_cloud, task_influx, return_exceptions=True)
                
            except Exception as e:
                logger.error(f"Error processing logic: {e}")

    async def _robust_read_csv(self, file_path: str) -> Optional[pd.DataFrame]:
        for _ in range(Config.MAX_RETRIES):
            try:
                if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                    return pd.read_csv(file_path)
            except: pass
            await asyncio.sleep(Config.RETRY_DELAY_SECONDS)
        return None


class CMMEventHandler(FileSystemEventHandler):
    def __init__(self, loop, processor: FileProcessor):
        self.loop = loop
        self.processor = processor

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith(".csv"):
            asyncio.run_coroutine_threadsafe(
                self.processor.process_file(event.src_path), 
                self.loop
            )

async def main():
    os.makedirs(Config.WATCH_DIR, exist_ok=True)
    
    # 1. Initialize Components
    cmm_server = CmmOpcServer()
    await cmm_server.init()
    
    cloud_connector = CloudConnector()
    await cloud_connector.init_session()
    
    influx_connector = InfluxConnector() # New Component

    # 2. Dependency Injection
    processor = FileProcessor(cmm_server, cloud_connector, influx_connector)

    # 3. Watchdog Setup
    loop = asyncio.get_running_loop()
    event_handler = CMMEventHandler(loop, processor)
    observer = Observer()
    observer.schedule(event_handler, path=Config.WATCH_DIR, recursive=False)
    
    try:
        await cmm_server.start()
        observer.start()
        logger.info("Sanmina Edge Gateway Running... (InfluxDB Connected)")
        while True: await asyncio.sleep(1)
            
    finally:
        observer.stop(); observer.join()
        await cmm_server.stop()
        await cloud_connector.close_session()
        await influx_connector.close() # Clean shutdown

if __name__ == "__main__":
    asyncio.run(main())