import logging
import smtplib
import ssl
from typing import Any


def send_email_notification(
    config: dict[str, Any],
    subject: str,
    body: str,
    log: logging.Logger,
) -> None:
    required_keys = ["smtp_username", "smtp_password", "smtp_to"]
    missing = [key for key in required_keys if not config.get(key)]
    if missing:
        log.info(
            "Email notification skipped. Missing SMTP config keys: %s",
            ", ".join(missing),
        )
        return

    smtp_host = config.get("smtp_host", "smtp.gmail.com")
    smtp_port = int(config.get("smtp_port", "587"))
    smtp_username = config["smtp_username"]
    smtp_password = config["smtp_password"]
    smtp_from = config.get("smtp_from", smtp_username)
    recipients = [
        recipient.strip()
        for recipient in config["smtp_to"].split(",")
        if recipient.strip()
    ]

    if not recipients:
        log.info("Email notification skipped. No recipients configured in smtp_to.")
        return

    headers = [
        f"From: {smtp_from}",
        f"To: {', '.join(recipients)}",
        f"Subject: {subject}",
        "MIME-Version: 1.0",
        "Content-Type: text/plain; charset=utf-8",
        "Content-Transfer-Encoding: 8bit",
    ]
    raw_message = ("\r\n".join(headers) + "\r\n\r\n" + body).encode(
        "utf-8", errors="replace"
    )

    context = ssl.create_default_context()
    with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        server.login(smtp_username, smtp_password)
        server.sendmail(smtp_from, recipients, raw_message)
