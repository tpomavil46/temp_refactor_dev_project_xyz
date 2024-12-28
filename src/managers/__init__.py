# src/__init__.py

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

# Set compatibility options
spy.options.compatibility = 192
spy.options.friendly_exceptions = False