import yaml
import numpy as np
import pandas as pd
import csv
import time
import json
import logging
import argparse

errors = []


# Argument Parser
parser = argparse.ArgumentParser(description="MLOps Batch Job")

parser.add_argument("--input", required=True, help="Input CSV file")
parser.add_argument("--config", required=True, help="Config YAML file")
parser.add_argument("--output", required=True, help="Output JSON file")
parser.add_argument("--log-file", required=True, help="Log file")

args = parser.parse_args()

print("Input file:", args.input)
print("Config file:", args.config)
print("Output file:", args.output)
print("Log file:", args.log_file)


# Logging Setup
logging.basicConfig(
    filename=args.log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

logger = logging.getLogger(__name__)

# Start timer
start_time = time.perf_counter()
logger.info("Job started")


# Validate Config
def validate_config(config_file):
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)

    if "seed" not in config or config["seed"] != 42:
        raise ValueError("Missing or incorrect value for seed")

    if "window" not in config or config["window"] != 5:
        raise ValueError("Missing or incorrect value for window")

    if "version" not in config or config["version"] != "v1":
        raise ValueError("Missing or incorrect value for version")

    logger.info("Config validated successfully")
    return config


try:
    config = validate_config(args.config)

    seed = config["seed"]
    np.random.seed(seed)

    print("Config loaded successfully")
    print("Random numbers from seed:", np.random.rand(4))

except Exception as e:
    print(e)
    errors.append(str(e))
    logger.error(str(e))



# Rolling Mean
def rolling_mean_cal(df):
    window = config["window"]
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    df["rolling_mean"] = df["close"].rolling(window=window).mean()
    df = df.dropna()

    logger.info("Rolling mean calculated")
    return df



# Signal Generation
def signal_generation(df):
    df = df.reset_index(drop=True)

    df["signal"] = np.where(df["close"] > df["rolling_mean"], 1, 0)

    logger.info("Signal generated")
    return df



# Latency Calculation
def latency_ms_cal(start_time, end_time):
    latency = end_time - start_time
    return round(latency * 1000)



# Read CSV
try:

    df = pd.read_csv(args.input, header=None)

    df = df[0].str.split(',', expand=True)
    df.columns = ['timestamp','open','high','low','close','volume_btc','volume_usd']
    actual_columns = df.columns.tolist()
    print(f"Actual Column: {actual_columns}")

    if 'close' not in df.columns:
        raise ValueError("Missing 'close' column in CSV")

    if "close" not in df.columns:
        raise ValueError("Missing 'close' column in CSV")

    df = rolling_mean_cal(df)
    print("Rolling mean calculated")

    df = signal_generation(df)
    print("Signal generated")

    rows_processed = df.shape[0]
    logger.info(f"Rows processed: {rows_processed}")

    signal_rate = round(df["signal"].mean(), 4)

    end_time = time.perf_counter()
    latency_ms = latency_ms_cal(start_time, end_time)

    print("Rows processed:", rows_processed)
    print("Signal rate:", signal_rate)
    print("Latency:", latency_ms, "ms")

except Exception as e:
    print(e)
    errors.append(str(e))
    logger.error(str(e))



# Output JSON
data = {
      "Success output":
    {
      "version":config['version'] ,
      "rows_processed": rows_processed,
      "metric": "signal_rate",
      "value": signal_rate,
      "latency_ms": latency_ms,
      "seed":config['seed'],
      "status": "success"
    },
     "Error output": {
      "version": config['version'],
      "status": "error",
      "error_message": errors
    }

}


try:

    with open(args.output, "w") as f:
        json.dump(data, f, indent=4)

    logger.info("Metrics file generated")

except Exception as e:
    print(e)
    logger.error(str(e))


if len(errors) == 0:
    logger.info("Job completed successfully")

else:
    logger.error("Job completed with errors")