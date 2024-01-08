import json
import pandas as pd
import yaml
import yfinance as yf

from datetime import datetime, timedelta
from loguru import logger
from pathlib import Path
from time import sleep
from typing import List

raw_data_path = Path("/home/vagrant/projekt/data/raw/finance/history")
last_ticks_timestamps_file = raw_data_path / "last_loaded_ticks.json"
companies_file = Path("/home/vagrant/projekt/config") / "companies.yaml"

def init_latest_ticks_if_not_exist(last_ticks_timestamps_file: Path, companies_to_load: List[str]) -> None:
    if not last_ticks_timestamps_file.exists():
        logger.info("Saving initial latest ticks file.")
        latest_ticks = {company: "1000-01-01 00:00:00" for company in companies_to_load}
        with open(last_ticks_timestamps_file, "w") as f:
            json.dump(latest_ticks, f, indent=4)

if __name__ == "__main__":
    logger.info("Initializing loading data from last year")
    raw_data_path.mkdir(exist_ok=True, parents=True)
    with open(companies_file) as f:
        companies_to_load = yaml.load(f, Loader=yaml.CLoader)
    init_latest_ticks_if_not_exist(last_ticks_timestamps_file, companies_to_load)
    
    
    logger.info("Starting loading data")
    current_timestamp = datetime.now()
    time_iterator = current_timestamp - timedelta(weeks=4)
    
    while time_iterator < current_timestamp:
        next_day = time_iterator + timedelta(days=1)
        logger.info(
            f"Starting loading data from "
            f"day {time_iterator.strftime('%Y-%m-%d')} "
            f"- {next_day.strftime('%Y-%m-%d')}"
        )
        
        for company in companies_to_load:
            logger.info(f"Loading company {company}")
            company_path = raw_data_path / company
            company_path.mkdir(exist_ok=True, parents=True)

            ticker = yf.Ticker(company)
            data = ticker.history(
                interval="1m",
                start=time_iterator.strftime("%Y-%m-%d"),
                end=next_day.strftime("%Y-%m-%d")
            )
            time = data.index + pd.DateOffset(hours=6)
            data.insert(0, "Time", time.astype(str).str[:-6])
            data = data.reset_index(drop=True)
            output_path = (
                company_path /
                f"{time_iterator.strftime('%Y-%m-%dT%H:%M:%S')}.csv"
            )
            if len(data) > 0:
                data.to_csv(output_path, index=False)
                logger.info("Updating last ticks file")
                with open(last_ticks_timestamps_file, "r") as f:
                    latest_ticks = json.load(f)
                latest_ticks[company] = latest_tick = data.Time.max()
                with open(last_ticks_timestamps_file, "w") as f:
                    json.dump(latest_ticks, f, indent=4)
            
            logger.info("Sleeping")
            sleep(0.5)
        time_iterator += timedelta(days=1)
