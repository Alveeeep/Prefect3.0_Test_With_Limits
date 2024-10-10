import time
from dotenv import load_dotenv
from prefect import task, flow
import pandas as pd
import requests
import os
from prefect.logging import get_run_logger
from prefect.concurrency.sync import concurrency, rate_limit

load_dotenv()


def send_telegram_message(message: str):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        raise ValueError("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is not set")

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {"chat_id": chat_id, "text": message}

    response = requests.post(url, data=data)

    if response.status_code != 200:
        raise Exception(f"Error sending message: {response.text}")


@task
def load_data_from_csv(path: str) -> pd.DataFrame:
    logger = get_run_logger()
    df = pd.read_csv(path, delimiter=";")
    logger.info(f"Loaded {len(df)} rows from {path}")
    return df


@task
def process_data(data: dict, symbol: str) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info(f"Processing data from API for symbol: {symbol}")
    df = pd.DataFrame.from_dict(data.get("Time Series (Daily)", {}), orient="index")
    df.reset_index(inplace=True)
    df.rename(columns={"index": "Date"}, inplace=True)
    return df


@task(retries=3, retry_delay_seconds=10)
def api_request(symbol: str) -> dict:
    logger = get_run_logger()
    logger.info(f"API request for {symbol}")
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey=demo"
    response = requests.get(url)
    response.raise_for_status()
    logger.info(f"API response: {response}")
    data = response.json()
    return data


@task
def save_data(df: pd.DataFrame, symbol: str, dir: str) -> None:
    logger = get_run_logger()
    os.makedirs(dir, exist_ok=True)
    path = os.path.join(dir, f"{symbol}.json")
    logger.info(f"Saving data to JSON file -> {path}")
    df.to_json(path, orient="records")
    logger.info(f"Saved data to JSON file -> {path}")


@task
def send_message(message: str) -> None:
    logger = get_run_logger()
    try:
        send_telegram_message(message)
        logger.info(f"Successfully sent message")
    except Exception as e:
        logger.error(f"Failed to send message")


@flow
def symbol_proccessing(symbol: str) -> None:
    rate_limit("symbol-proccessing", occupy=1)
    request = api_request.submit(symbol)
    data = request.result()
    df = process_data.submit(data, symbol)
    df = df.result()
    save_data.submit(df, symbol, f"results")


@flow
def main_flow() -> None:
    logger = get_run_logger()
    logger.info(f"Starting main flow")
    df = load_data_from_csv.submit("CSV.csv")
    df = df.result()
    for symbol in df["symbol"].unique():
        symbol_proccessing(symbol)
    send_message.submit("Main flow completed successfully")
    logger.info(f"Finished main flow")


if __name__ == "__main__":
    main_flow.from_source(
        source=".",
        entrypoint="main.py:main_flow"
    ).deploy(
        name="main-flow-deployment",
        work_pool_name="process-work-pool",
        work_queue_name="api-queue",
    )
