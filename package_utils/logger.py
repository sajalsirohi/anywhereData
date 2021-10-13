import logging


def prepare_logging(dt_string, log_path):
    """
    Prepare the logging object
    :param dt_string: Current time as string
    :param log_path: path where the logs will be saved
    :return: None
    """
    print("creating log file")
    # Remove all handlers associated with the root logger object.
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        format='%(levelname)s - %(asctime)s - %(message)s',
        datefmt='%d-%b-%y %H:%M:%S',
        level=logging.INFO,
        filename=rf"{log_path}/Anywhere_Data_{dt_string}.log",
        )
    return rf"{log_path}/Anywhere_Data_{dt_string}.log"