import shutil
import seeq
from seeq import spy
import pandas as pd
import numpy as np
import json
import os
import csv
from seeq.spy.assets import Tree, Asset
from seeq.spy import search
from pprint import pprint
from typing import Optional
import re
from dotenv import load_dotenv

# Load the .env file
load_dotenv()

# Access environment variables
username = os.getenv("SERVER_USERNAME")
password = os.getenv("SERVER_PASSWORD")
host = os.getenv("SERVER_HOST")

# Print connection information (optional, avoid printing sensitive data in production)
print(f"Connecting to {host} as {username}...")

# Set the compatibility option so that you maximize the chance that SPy will remain compatible with your notebook/script
spy.options.compatibility = 193
spy.options.friendly_exceptions = False

# Use spy.login with credentials from .env
spy.login(url=host, username=username, password=password)