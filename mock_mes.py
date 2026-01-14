import uvicorn
from fastapi import FastAPI, Request
from pydantic import BaseModel
import logging
import random
import asyncio

# --- Configuration ---
PORT = 8000
HOST = "127.0.0.1"

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [MOCK MES] - %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("MockMES")

app = FastAPI()

class MeasurementPayload(BaseModel):
    machine_id: str
    timestamp: str
    temperature: float
    length: float
    status: str

@app.post("/api/v1/cmm/measurements")
async def receive_measurement(payload: MeasurementPayload):
    """
    Simulates receiving data at the 42Q Cloud API.
    """
    # Simulates a tiny bit of cloud latency (non-blocking)
    await asyncio.sleep(random.uniform(0.05, 0.2))
    
    if payload.status == "FAIL":
        logger.warning(f"RECEIVED ALARM: {payload.machine_id} | Temp: {payload.temperature}C | Status: {payload.status}")
    else:
        logger.info(f"Received: {payload.machine_id} | Len: {payload.length} | Status: {payload.status}")

    return {"status": "success", "msg": "Data ingested"}

if __name__ == "__main__":
    logger.info(f"Starting Mock MES Server on http://{HOST}:{PORT}")
    uvicorn.run(app, host=HOST, port=PORT)