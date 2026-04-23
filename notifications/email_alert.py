"""
Send an email digest of active data signals.
Queries the signals tables in Snowflake and sends a summary email.
"""
import os
import smtplib
import snowflake.connector
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../config/.env"))

SMTP_HOST     = os.environ["SMTP_HOST"]
SMTP_PORT     = int(os.environ.get("SMTP_PORT", 587))
SMTP_USER     = os.environ["SMTP_USER"]
SMTP_PASSWORD = os.environ["SMTP_PASSWORD"]
EMAIL_FROM    = os.environ["EMAIL_FROM"]
EMAIL_TO      = os.environ["EMAIL_TO"]   # comma-separated for multiple recipients

SNOWFLAKE_CONFIG = {
    "account":   os.environ["SNOWFLAKE_ACCOUNT"],
    "user":      os.environ["SNOWFLAKE_USER"],
    "password":  os.environ["SNOWFLAKE_PASSWORD"],
    "role":      os.environ["SNOWFLAKE_ROLE"],
    "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
    "database":  os.environ["SNOWFLAKE_DATABASE"],
    "schema":    os.environ["SNOWFLAKE_SCHEMA"],
}

SIGNAL_TABLES = [
    "signals.sig_ctr_drop",
    "signals.sig_spend_spike",
]


def fetch_signals(conn, table: str) -> list[dict]:
    cursor = conn.cursor(snowflake.connector.DictCursor)
    cursor.execute(f"SELECT * FROM {table}")
    return cursor.fetchall()


def build_html_table(rows: list[dict]) -> str:
    if not rows:
        return "<p>No signals.</p>"
    headers = list(rows[0].keys())
    header_html = "".join(f"<th>{h}</th>" for h in headers)
    rows_html = ""
    for row in rows:
        cells = "".join(f"<td>{row[h]}</td>" for h in headers)
        rows_html += f"<tr>{cells}</tr>"
    return (
        "<table border='1' cellpadding='4' cellspacing='0'>"
        f"<thead><tr>{header_html}</tr></thead>"
        f"<tbody>{rows_html}</tbody>"
        "</table>"
    )


def send_email(subject: str, html_body: str):
    recipients = [r.strip() for r in EMAIL_TO.split(",")]
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = EMAIL_FROM
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(EMAIL_FROM, recipients, msg.as_string())


def main():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    sections = []
    total_signals = 0

    try:
        for table in SIGNAL_TABLES:
            signals = fetch_signals(conn, table)
            total_signals += len(signals)
            table_label = table.split(".")[-1]
            sections.append(f"<h3>{table_label}</h3>" + build_html_table(signals))
    finally:
        conn.close()

    if total_signals == 0:
        print("No signals found, skipping email.")
        return

    html = f"""
    <html><body>
        <h2>Data Signals Report</h2>
        <p><strong>{total_signals}</strong> signal(s) detected.</p>
        {"".join(sections)}
    </body></html>
    """
    send_email(subject=f"[Data Signals] {total_signals} signal(s) detected", html_body=html)
    print(f"Email sent: {total_signals} signal(s).")


if __name__ == "__main__":
    main()
