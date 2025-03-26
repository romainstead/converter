import logging

import yaml
from pathlib import Path
import dotenv
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from datetime import datetime, UTC

parent_dir = Path(__file__).parent.parent.parent
converted_dir = parent_dir / 'converted'
combined_dir = parent_dir / 'combined'
cfg_path = parent_dir / 'cfg' / 'config.yaml'

dotenv.load_dotenv()
# Логирование
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt="%Y-%m-%dT%H:%M:%S %Z",
)

logger = logging.getLogger(__name__)
logging.Formatter.converter = lambda *args: datetime.now(UTC).timetuple()


def load_config(cfg_path):
    with open(cfg_path, 'r') as cfg:
        return yaml.safe_load(cfg)['config']


def attach_files(msg, directory):
    for file in os.listdir(directory):
        filepath = os.path.join(directory, file)
        with open(filepath, "rb") as f:
            part = MIMEApplication(f.read())
            part["Content-Disposition"] = f'attachment; filename="{file}"'
            msg.attach(part)


def send_files_smtp(directory, subject, cfg):
    today = datetime.now(UTC).date()
    BODY = ""
    SUBJECT = f"{subject}_{today}"
    SENDER = cfg['SENDER_EMAIL']
    RECEIVER = cfg['RECEIVER_EMAIL']
    PASSWORD = os.getenv("SMTP_PASSWORD")
    PORT = cfg['SMTP_PORT']
    SERVER = cfg['SMTP_SERVER']

    msg = MIMEMultipart()
    msg["From"] = SENDER
    msg["To"] = RECEIVER
    msg["Subject"] = SUBJECT
    msg.attach(MIMEText(BODY, "plain"))
    attach_files(msg, directory)
    try:
        with smtplib.SMTP(SERVER, PORT) as server:
            server.starttls()
            server.login(SENDER, PASSWORD)
            server.sendmail(SENDER, RECEIVER, msg.as_string())
    except Exception as e:
        print(e)


config = load_config(cfg_path)

if __name__ == "__main__":
    send_files_smtp(converted_dir, "converted_files", config)
    send_files_smtp(combined_dir, "combined_files", config)
