import os, sys
from fetch_data import build_datasets
from utils import logging, CustomException

if __name__ == "__main__":
    # Grand Prix Parameters
    gp_year = 2023
    gp_circuit_name = "Yas Marina Circuit"
    gp_location = "AbuDhabi"
    session_type = "Race"

    # ensure paths - ensure directory exists
    # os.chdir("..")
    DATA_DIR = os.path.join("data", "main")
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # file names
    lap_main_path = os.path.join(DATA_DIR, f"laps_all_{gp_year}_{gp_location}_{session_type[0]}.csv")
    micro_telemetry_path = os.path.join(DATA_DIR, f"telemetry_micro_all_{gp_year}_{gp_location}_{session_type[0]}_m100.csv")

    try:
        # build datasets
        df_laps, df_telemetry = build_datasets(
            year=gp_year, circuit_name=gp_circuit_name
        )

        # store as csv files
        df_telemetry.to_csv(micro_telemetry_path, index=False)
        logging.info(f"Success! Micro-telemetry data saved to {micro_telemetry_path}")

        df_laps.to_csv(lap_main_path, index=False)
        logging.info(f"Success! Lap data saved to {lap_main_path}")

        logging.info("=====Data Collection execution finished=====")

    except Exception as e:
        logging.info(e)
        raise CustomException(e, sys)
    else:
        print(f"Success! Lap data data is available at {lap_main_path}")
        print(f"Success! Micro-telemetry data is available at {micro_telemetry_path}")
    finally:
        print("=====Data Collection execution finished=====")