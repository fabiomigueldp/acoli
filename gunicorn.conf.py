import multiprocessing
import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent

env_path = BASE_DIR / ".env"
if env_path.exists():
    load_dotenv(env_path)

workers = 2
threads = 4
max_requests = 1000
max_requests_jitter = 50

