import asyncio
import logging
import os
import time
import pandas as pd
from typing import Optional
from collections import deque # Optimized double-ended queue for Store & Forward

import aiohttp # Async HTTP client (CRITICAL: Do not use 'requests' library)
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from asyncua import Server, ua

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

    # IT / Cloud Settings (Phase 3)
    MES_API_URL = "http://127.0.0.1:8000/api/v1/cmm/measurements"
    MACHINE_ID = "SANMINA_CMM_01"
    MAX_BUFFER_SIZE = 1000 # Max items to store offline before dropping old data

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("EdgeGateway")


class CloudConnector:
    """
    Handles HTTP communication with the MES.
    Implements 'Store and Forward' logic for network resilience.
    """
    def __init__(self):
        self._buffer = deque(maxlen=Config.MAX_BUFFER_SIZE)
        self.session = None # aiohttp session

    async def init_session(self):
        """Create a persistent HTTP session."""
        self.session = aiohttp.ClientSession()

    async def close_session(self):
        """Clean up the session."""
        if self.session:
            await self.session.close()

    async def send_measurement(self, payload: dict):
        """
        Public method to send data. 
        Tries to send immediately. If fail, stores in buffer.
        If success, tries to flush buffer.
        """
        # 1. Try to send the current payload
        sent = await self._post_to_cloud(payload)

        if not sent:
            self._buffer.append(payload)
            logger.warning(f"Network Connection Lost! Buffering payload locally. Buffer Size: {len(self._buffer)}")
        else:
            # 2. If successful, check if we have a backlog to clear
            if len(self._buffer) > 0:
                logger.info(f"Network Restored! Flushing {len(self._buffer)} buffered items...")
                await self._flush_buffer()

    async def _post_to_cloud(self, payload: dict) -> bool:
        """
        Internal method to perform the actual HTTP POST.
        Returns True if success, False if network error.
        """
        if self.session is None:
            await self.init_session()

        try:
            async with self.session.post(Config.MES_API_URL, json=payload) as response:
                if response.status == 200:
                    return True
                else:
                    logger.error(f"MES returned error {response.status}: {await response.text()}")
                    return False # We treated server errors as 'send failed' for simplicity
        except aiohttp.ClientConnectorError:
            # This is the specific error for "Server Down" or "No Internet"
            return False
        except Exception as e:
            logger.error(f"Unexpected HTTP error: {e}")
            return False

    async def _flush_buffer(self):
        """
        Attempts to empty the buffer. 
        Stops if connection fails again to preserve order.
        """
        while len(self._buffer) > 0:
            # Peek at the oldest item (FIFO)
            item = self._buffer[0]
            
            success = await self._post_to_cloud(item)
            if success:
                self._buffer.popleft() # Remove from queue only on success
                logger.debug("Buffered item flushed successfully.")
            else:
                logger.warning("Network unstable during flush. Stopping flush.")
                break # Stop trying, keep items in queue


class CmmOpcServer:
    # ... (Same as Phase 2, keeping it brief for the file) ...
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
            logger.info(f"OPC-UA Updated: [{status}] Temp={temp}")
        except Exception as e:
            logger.error(f"OPC Error: {e}")


class FileProcessor:
    def __init__(self, opc_server: CmmOpcServer, cloud_connector: CloudConnector):
        self.opc_server = opc_server
        self.cloud_connector = cloud_connector # Inject the new dependency

    async def process_file(self, file_path: str):
        # ... (Retry/Debounce logic same as Phase 2) ...
        df = await self._robust_read_csv(file_path)
        
        if df is not None and not df.empty:
            try:
                latest = df.iloc[-1]
                temp = float(latest['Simulated_Temperature_C'])
                length = float(latest['Observed_Part_Length_mm'])
                timestamp = str(latest['Timestamp'])

                # 1. Edge Analytics
                status = "FAIL" if temp > Config.MAX_TEMP_THRESHOLD_C else "ACTIVE"

                # 2. Update OT Layer (OPC-UA)
                # We use asyncio.gather to run OT and IT updates in parallel!
                opc_task = self.opc_server.update_values(temp, length, timestamp, status)

                # 3. Update IT Layer (Cloud/MES)
                json_payload = {
                    "machine_id": Config.MACHINE_ID,
                    "timestamp": timestamp,
                    "temperature": temp,
                    "length": length,
                    "status": status
                }
                cloud_task = self.cloud_connector.send_measurement(json_payload)

                # Wait for both (or just fire and forget if speed is critical, but await is safer)
                await asyncio.gather(opc_task, cloud_task)
                
            except Exception as e:
                logger.error(f"Error processing data: {e}")

    async def _robust_read_csv(self, file_path: str) -> Optional[pd.DataFrame]:
        # (Same Phase 2 logic here)
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
    
    # Init Components
    cmm_server = CmmOpcServer()
    await cmm_server.init()
    
    cloud_connector = CloudConnector()
    await cloud_connector.init_session() # Start HTTP session

    processor = FileProcessor(cmm_server, cloud_connector)

    # Watchdog Setup
    loop = asyncio.get_running_loop()
    event_handler = CMMEventHandler(loop, processor)
    observer = Observer()
    observer.schedule(event_handler, path=Config.WATCH_DIR, recursive=False)
    
    try:
        await cmm_server.start()
        observer.start()
        logger.info("Sanmina Edge Gateway Running... (Press Ctrl+C to Stop)")
        while True: await asyncio.sleep(1)
            
    finally:
        observer.stop(); observer.join()
        await cmm_server.stop()
        await cloud_connector.close_session()

if __name__ == "__main__":
    asyncio.run(main())