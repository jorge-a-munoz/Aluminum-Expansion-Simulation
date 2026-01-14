import asyncio
import logging
import os
import time
import datetime
import numpy as np
import pandas as pd

class Config:
    """
    Separate class for constants to make it easier to manage and change.
    """
    NOMINAL_PART_LENGTH_MM: float = 100.0
    REFERENCE_TEMPERATURE_C: float = 20.0
    
    CTE_ALUMINUM_6061: float = 23.6e-6
    
    # CMM Noise Params
    NOISE_MEAN_MM: float = 0.0
    NOISE_STD_DEV_MM: float = 0.001

    # Temperature Drift Simulation
    TEMP_SINE_AMPLITUDE_C: float = 5.0
    TEMP_SINE_PERIOD_SECONDS: float = 60.0
    TEMP_OFFSET_C: float = REFERENCE_TEMPERATURE_C
    
    # CSV Output Parameters
    CSV_OUTPUT_DIR: str = "cmm_data_output"
    CSV_INTERVAL_SECONDS: int = 5
    MAX_CSV_FILES: int = 20

# Structured logger for debugging and auditing
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()
    ],
)
logger = logging.getLogger("CMMSimulator")

class PhysicsEngine:
    """
    Calculates the true dimension of a material based on thermal expansion.
    """

    @staticmethod
    def calculate_expanded_length(
        nominal_length_mm: float,
        base_temp_c: float,
        current_temp_c: float,
        coeff_thermal_expansion: float,
    ) -> float:
        """
        Calculates expanded/contracted length of a material
        based on thermal expansion.
        """
        if any(arg is None for arg in [nominal_length_mm, base_temp_c, current_temp_c, coeff_thermal_expansion]):
            logger.warning("PhysicsEngine received None for one or more arguments. Returning nominal length.")
            return nominal_length_mm

        try:
            delta_temp = current_temp_c - base_temp_c
            expanded_length = nominal_length_mm * (1 + coeff_thermal_expansion * delta_temp)
            return expanded_length
        except Exception as e:
            logger.error(f"Error calculating expanded length: {e}. Returning nominal length.")
            return nominal_length_mm

class CMMSimulator:
    """
    Simulates a CMM Machine generating data with thermal expansion and sensor noise.
    Outputs data to CSV files periodically.
    """

    def __init__(
        self,
        nominal_length: float,
        reference_temp: float,
        cte: float,
        noise_mean: float,
        noise_std_dev: float,
        temp_sine_amplitude: float,
        temp_sine_period: float,
        temp_offset: float,
        output_dir: str,
        max_csv_files: int,
    ):
        self.nominal_length = nominal_length
        self.reference_temp = reference_temp
        self.cte = cte
        self.noise_mean = noise_mean
        self.noise_std_dev = noise_std_dev
        self.temp_sine_amplitude = temp_sine_amplitude
        self.temp_sine_period = temp_sine_period
        self.temp_offset = temp_offset
        self.output_dir = output_dir
        self.max_csv_files = max_csv_files

        os.makedirs(self.output_dir, exist_ok=True)
        logger.info(f"CMM Simulator initialized. Output directory: '{self.output_dir}'")

    def _generate_temperature(self, current_time_seconds: float) -> float:
        """
        Generates a sinusoidal temperature value based on current time.
        """
        sine_wave = self.temp_sine_amplitude * np.sin(
            (2 * np.pi / self.temp_sine_period) * current_time_seconds
        )
        return self.temp_offset + sine_wave

    def _add_gaussian_noise(self, value: float) -> float:
        """
        Adds Guassian (normal) distributed noise to a given value
        """
        noise = np.random.normal(self.noise_mean, self.noise_std_dev)
        return value + noise

    async def generate_and_save_data(self):
        """
        Generates a single data point and saves it to a timestamped CSV file.
        """
        current_timestamp = time.time()
        start_time = datetime.datetime.fromtimestamp(current_timestamp - (Config.CSV_INTERVAL_SECONDS / 2))

        elapsed_time_seconds = current_timestamp

        current_temperature = self._generate_temperature(elapsed_time_seconds)

        true_length = PhysicsEngine.calculate_expanded_length(
            self.nominal_length,
            self.reference_temp,
            current_temperature,
            self.cte,
        )

        observed_length = self._add_gaussian_noise(true_length)

        data = {
            "Timestamp": datetime.datetime.fromtimestamp(current_timestamp).isoformat(),
            "Simulated_Temperature_C": round(current_temperature, 3),
            "True_Part_Length_mm": round(true_length, 4),
            "Observed_Part_Length_mm": round(observed_length, 4),
        }
        df = pd.DataFrame([data])

        timestamp_str = datetime.datetime.fromtimestamp(current_timestamp).strftime(
            "%Y%m%d_%H%M%S_%f"
        )
        filepath = os.path.join(self.output_dir, f"cmm_reading_{timestamp_str}.csv")

        try:
            df.to_csv(filepath, index=False)
            logger.info(f"Generated CMM data to '{filepath}': Temp={current_temperature:.2f}C, Obs Length={observed_length:.4f}mm")
        except IOError as e:
            logger.error(f"Failed to Write CSV to {filepath}: {e}")
            return
        
        self._clean_old_csv_files()

    def _clean_old_csv_files(self):
        """
        Deletes oldest CSV files if the number of files exceeds max_csv_files
        """
        files = sorted(
            [
                os.path.join(self.output_dir, f)
                for f in os.listdir(self.output_dir)
                if f.startswith("cmm_reading_") and f.endswith(".csv")
            ],
            key = os.path.getmtime,
        )

        if len(files) > self.max_csv_files:
            num_to_delete = len(files) - self.max_csv_files
            for i in range(num_to_delete):
                try:
                    os.remove(files[i])
                    logger.debug(f"Deleted old CMM file: {files[i]}")
                except OSError as e:
                    logger.error(f"Error deleting file {files[i]}: {e}")

async def main():
    """
    Main asynchronous function to run the CMM simulator
    """
    simulator = CMMSimulator(
        nominal_length=Config.NOMINAL_PART_LENGTH_MM,
        reference_temp=Config.REFERENCE_TEMPERATURE_C,
        cte=Config.CTE_ALUMINUM_6061,
        noise_mean=Config.NOISE_MEAN_MM,
        noise_std_dev=Config.NOISE_STD_DEV_MM,
        temp_sine_amplitude=Config.TEMP_SINE_AMPLITUDE_C,
        temp_sine_period=Config.TEMP_SINE_PERIOD_SECONDS,
        temp_offset=Config.TEMP_OFFSET_C,
        output_dir=Config.CSV_OUTPUT_DIR,
        max_csv_files=Config.MAX_CSV_FILES,
    )

    logger.info(
        f"Starting CMM Simulation. Generating CSV every {Config.CSV_INTERVAL_SECONDS} seconds..."
    )
    logger.info("Press Ctrl+C to stop the simulator.")

    # Main loop for periodic CSV generation
    while True:
        await simulator.generate_and_save_data()
        await asyncio.sleep(Config.CSV_INTERVAL_SECONDS)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("CMM Simulator stopped by user (Ctrl+C). Exiting.")
    except Exception as e:
        logger.exception(f"An unexpected error ocurred in the main loop: {e}")