# MLOps Batch Processing Task

## Overview
This project implements a minimal MLOps-style batch job in Python.  
It processes OHLCV data, computes a rolling mean on the `close` column, generates trading signals, and outputs structured metrics and logs.

---

## Features
- Config-driven execution using YAML
- Deterministic runs using seed
- Rolling mean calculation
- Binary signal generation
- Structured metrics output (JSON)
- Detailed logging
- Dockerized for reproducibility
---
## Run the Project

```bash
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log
---

## Docker Image

-You can pull the pre-built Docker image:

```bash
docker pull aryanmashalkar/mlops-task
