import json
import logging
import traceback

from os import listdir
from os.path import isfile, join

import pandas as pd
import yaml


def read_file(file_path, extension='yaml', from_directory=False, **options):
    """
    Read the INI file
    :param from_directory: if needed from multiple files, give the directory path
    :param extension: yaml or json
    :param file_path:
    :return: dict
    """
    logging.info(f"Trying to read file from : {file_path}")
    try:
        # if we have to read a single file
        if not from_directory:
            with open(file_path, 'r') as f:
                if extension == "yaml":
                    config = yaml.safe_load(f)
                    return config
                elif extension == "json":
                    # if we want json result, as a pandas df
                    if options.get('get_as_pd_df'):
                        logging.info(f"Turning the result to pandas df")
                        result = pd.DataFrame(json.load(f))
                        result.columns = [col.upper() for col in list(result.columns)]
                        return result
                    else:
                        logging.info("Returning the result json")
                        return json.load(f)
                elif extension == "txt":
                    f = open(file_path, "r")
                    return f.read()
        else:
            # if directories are mentioned
            if from_directory:
                # final json docs will be stored here
                final_result = []
                # if file_path = "", then change it to [""], so multiple directories can be walked over here
                if not isinstance(file_path, list):
                    file_path = [file_path]
                for dir_path in file_path:
                    for file in [join(dir_path, f) for f in listdir(dir_path) if isfile(join(dir_path, f))]:
                        with open(file, 'r') as f:
                            final_result += json.load(f)

                # if we want result back in form of a pandas df
                if options.get('get_as_pd_df'):
                    logging.info(f"Turning the result to pandas df")
                    result_as_df = pd.DataFrame(final_result)
                    # upper case all of the columns
                    result_as_df.columns = [col.upper() for col in list(result_as_df.columns)]
                    return result_as_df
                else:
                    return final_result

    except FileNotFoundError as err:
        if options.get('ignore_error'):
            logging.info(f"Ignoring the error of File Not Found")
            return None
        logging.error(f"File not found at : {file_path}")
        raise err

    except Exception as err:
        logging.error(f"Error occurred while reading the file at path : {file_path}\n Error trace "
                      f"- {traceback.format_exc()}")
        raise err


def prepare_connection_config(yaml_config, file_name) -> list:
    """
    Returns the config which is acceptable by the connection pool
    It takes direct yaml_config which is defined in 'connections.yaml'
    """
    final_result = []
    logging.info(f"Starting to prepare the {file_name} config")
    config = yaml_config[file_name]
    for connection_name, properties in config.items():
        logging.info(f"Gathering the config for connection name : {connection_name}")
        final_result.append(properties)
    return final_result


def read_config_files(file_name, **options):
    """
    Read the config files
    """
    connections_yaml = read_file(file_name, ignore_error=True, **options)
    return prepare_connection_config(connections_yaml, options.get('key')) if connections_yaml else None