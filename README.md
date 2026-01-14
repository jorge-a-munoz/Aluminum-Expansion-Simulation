# Aluminum Expansion Simulation

This project simulates aluminum expansion and provides an OPC-UA interface for sensor data.

## Project Structure

- `cmm_simulator.py`: Simulates CMM (Coordinate Measuring Machine) readings for aluminum parts.
- `cmm_edge_gateway.py`: OPC-UA server that exposes the simulation data.
- `mock_mes.py`: A mock Manufacturing Execution System for testing.

## Getting Started

1. Create a virtual environment:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```
2. Install dependencies (if any):
   ```bash
   pip install -r requirements.txt
   ```
3. Run the simulator or gateway:
   ```bash
   python cmm_edge_gateway.py
   ```
