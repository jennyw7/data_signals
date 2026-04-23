"""
Send Slack alerts for active data signals.
Queries the signals tables in Snowflake and posts a message per signal found.
"""
import os
import snowflake.connector
import requests
from dotenv import load_dotenv
import yaml

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../config/.env"))

SLACK_WEBHOOK_URL = os.environ["SLACK_WEBHOOK_URL"]
SNOWFLAKE_CONFIG = {
    "account":   os.environ["SNOWFLAKE_ACCOUNT"],
    "user":      os.environ["SNOWFLAKE_USER"],
    "password":  os.environ["SNOWFLAKE_PASSWORD"],
    "role":      os.environ["SNOWFLAKE_ROLE"],
    "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
    "database":  os.environ["SNOWFLAKE_DATABASE"],
    "schema":    os.environ["SNOWFLAKE_SCHEMA"],
}

# Signal tables to query (schema.table_name)
SIGNAL_TABLES = [
    "signals.sig_ctr_drop",
    "signals.sig_spend_spike",
]


def load_thresholds():
    path = os.path.join(os.path.dirname(__file__), "../config/thresholds.yml")
    with open(path) as f:
        return yaml.safe_load(f)


def fetch_signals(conn, table: str) -> list[dict]:
    cursor = conn.cursor(snowflake.connector.DictCursor)
    cursor.execute(f"SELECT * FROM {table}")
    return cursor.fetchall()


def format_message(signal: dict) -> str:
    signal_type = signal.get("SIGNAL_TYPE", "unknown")
    campaign = signal.get("CAMPAIGN_NAME", signal.get("CAMPAIGN_ID", "?"))

    if signal_type == "ctr_drop":
        pct = round(signal["PCT_CHANGE"] * 100, 1)
        return (
            f":rotating_light: *CTR Drop* | Campaign: *{campaign}*\n"
            f"CTR dropped *{pct}%* vs prior week "
            f"({round(signal['PREV_CTR']*100,2)}% → {round(signal['CTR']*100,2)}%)"
        )
    elif signal_type == "spend_spike":
        ratio = round(signal["SPEND_RATIO"], 2)
        return (
            f":money_with_wings: *Spend Spike* | Campaign: *{campaign}*\n"
            f"Today's spend is *{ratio}x* the 7-day average "
            f"(${round(signal['SPEND'],2)} vs avg ${round(signal['AVG_SPEND_7D'],2)})"
        )
    else:
        return f":warning: Signal fired: `{signal_type}` for campaign `{campaign}`"


def post_to_slack(message: str):
    response = requests.post(SLACK_WEBHOOK_URL, json={"text": message}, timeout=10)
    response.raise_for_status()


def main():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    try:
        for table in SIGNAL_TABLES:
            signals = fetch_signals(conn, table)
            if not signals:
                print(f"No signals in {table}")
                continue
            for signal in signals:
                msg = format_message(signal)
                post_to_slack(msg)
                print(f"Sent: {msg[:80]}...")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
