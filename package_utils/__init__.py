import pathlib
import os
from datetime import datetime as dt

from .argument_parser import additional_options
from .design_patterns import Singleton
from .logger import prepare_logging
from .package_utils import read_file, prepare_connection_config, read_config_files
from .sparkSession import spark

ROOT_DIR = pathlib.Path(__file__).parent.parent.resolve()
CONFIG_DIR = additional_options.get('config_dir', os.path.join(ROOT_DIR, 'config'))

# is used in log file name
dt_string = dt.strftime(dt.utcnow(), "%Y-%m-%d_%H_%M_%S")
prepare_logging(dt_string, additional_options.get('log_path', os.path.join(ROOT_DIR, 'logs')))

# read the connection config files
connections_config = read_config_files(os.path.join(CONFIG_DIR, 'connections.yaml'), key='connections')

# read the tasks yaml file
tasks_config = read_config_files(os.path.join(CONFIG_DIR, 'tasks.yaml'), key='tasks')

# pipeline config yaml file
pipeline_config = read_config_files(os.path.join(CONFIG_DIR, 'pipeline.yaml'), key='pipeline')
