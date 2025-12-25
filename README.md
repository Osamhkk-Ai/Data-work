Data Work — ETL + EDA Project

This project demonstrates an end-to-end data workflow:
loading raw data, cleaning and transforming it, building analytics outputs,
and exploring results using an EDA notebook.

==================================================

Project Structure
==================================================

Data-work/
  data/
    raw/
    processed/
  scripts/
    run_day1_load.py
    run_day2_clean.py
    run_day3_build.py
    run_etl.py
  src/
    bootcamp_data/
  notebooks/
    eda.ipynb
  reports/
    figures/
  requirements.txt
  README.txt

==================================================

Setup
==================================================

cd Data-work
uv venv -p 3.11
source .venv/bin/activate
# On Windows:
# .venv\Scripts\activate

uv pip install -r requirements.txt

Python version: 3.11  
Environment manager: uv

==================================================

Environment Variables
==================================================

Required if using src/ layout.

Mac / Linux:
export PYTHONPATH=src

Windows (PowerShell):
$env:PYTHONPATH="src"

==================================================

Run ETL
==================================================

Recommended (full pipeline):

uv run python scripts/run_etl.py

This runs the full pipeline:
- load raw data
- clean and transform datasets
- build analytics tables
- write all processed outputs

----------------------------------------------

Step-by-step (optional):

uv run python scripts/run_day1_load.py
uv run python scripts/run_day2_clean.py
uv run python scripts/run_day3_build.py

==================================================

Outputs
==================================================

data/processed/orders_clean.parquet  
data/processed/users.parquet  
data/processed/analytics_table.parquet  
data/processed/_run_meta.json  
reports/figures/*.png  

==================================================

EDA
==================================================

Open notebooks/eda.ipynb and run all cells to reproduce the charts.
