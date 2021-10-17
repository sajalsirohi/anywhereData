from datetime import datetime as dt
from .logger import prepare_logging
from .design_patterns import Singleton

import pathlib
ROOT_DIR = pathlib.Path(__file__).parent.parent.resolve()

dt_string = dt.strftime(dt.utcnow(), "%Y-%m-%d_%H_%M_%S")
prepare_logging(dt_string, ROOT_DIR)

from .package_utils import read_file
