import json
import pandas as pd
import pytz
import yaml
import yfinance as yf

from datetime import datetime, timedelta
from loguru import logger
from pathlib import Path
from threading import Lock, Thread
from time import sleep

raw_data_path = Path("/home/vagrant/projekt/data/raw/finance/online")
last_ticks_timestamps_file = raw_data_path / "last_loaded_ticks.json"
companies_file = Path("/home/vagrant/projekt/config") / "companies.yaml"
tick_update_lock = Lock()


def init_latest_ticks_if_not_exist(last_ticks_timestamps_file: Path) -> None:
    if not last_ticks_timestamps_file.exists():
        logger.info("Saving initial latest ticks file.")
        latest_ticks = {company: "1000-01-01 00:00:00" for company in companies_to_load}
        with open(last_ticks_timestamps_file, "w") as f:
            json.dump(latest_ticks, f, indent=4)


def load_newest_data_increment_for_company(company: str, period: str, interval="1m"):
    ticker = yf.Ticker(company)
    data = ticker.history(period=period, interval=interval)
    time = data.index + pd.DateOffset(hours=6)
    data.insert(0, "Time", time.astype(str).str[:-6])
    data = data.reset_index(drop=True)
    return data


def company_data_load(company: str):
    logger.info(f"{company} - Starting loading data proces")
    data_path = raw_data_path / company
    data_path.mkdir(exist_ok=True, parents=True)

    while True:
        logger.info(f"{company} - Loading data")
        latest_tick = latest_ticks[company]

        current_time = datetime.utcnow()
        logger.info(f"{company} - Current time={current_time}")
        
        data = load_newest_data_increment_for_company(company, "1d")
        data = data.loc[
            (data.Time > latest_tick) &
            (data.Time <= current_time.strftime('%Y-%m-%d %H:%M:%S'))
        ]

        logger.info(f"{company} - Data size = {len(data)}")
        if len(data) > 0:
            target_file = data_path / f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}.csv"
            logger.info(f"{company} - Target file = {target_file}")
            data.to_csv(target_file, index=False)
            logger.info(f"{company} - Updating newest tick timestamp")
            latest_tick = data.Time.max()
            with tick_update_lock:
                latest_ticks[company] = latest_tick
                with open(last_ticks_timestamps_file, "w") as f:
                    json.dump(latest_ticks, f, indent=4)
            logger.info(f"{company} - Updated newest tick timestamp - {latest_tick}")
        
        sleep(60)
        

def latest_ticks_periodic_check():
    while True:
        with open(last_ticks_timestamps_file) as f:
            latest_ticks = json.load(f)
        logger.info(f"Latest load ticks: {json.dumps(latest_ticks, indent=4)}")
        sleep(2*60)

if __name__ == "__main__":
    logger.info("Initializing financial data loading process.")
    raw_data_path.mkdir(exist_ok=True, parents=True)
    with open(companies_file) as f:
        companies_to_load = yaml.load(f, Loader=yaml.CLoader)
    logger.info(f"Companies to be loaded: {companies_to_load}")

    logger.info("Obtaining newest ticks dates.")
    init_latest_ticks_if_not_exist(last_ticks_timestamps_file)
    with open(last_ticks_timestamps_file) as f:
        latest_ticks = json.load(f)

    Thread(target=latest_ticks_periodic_check).start()
    for company in companies_to_load:
        Thread(target=company_data_load, args=(company,)).start()
        sleep(5)
