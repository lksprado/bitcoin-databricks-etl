import logging
import os

def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        ))
        logger.addHandler(stream_handler)

        # CAMINHO COMPATÍVEL COM DBFS
        log_dir = "/Workspace/Users/lks-prado@live.com/bitcoin-databricks-etl/logs"
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, "etl.log")

        file_handler = logging.FileHandler(log_file, mode='a', encoding="utf-8")
        file_handler.setFormatter(logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        ))
        logger.addHandler(file_handler)

    return logger
