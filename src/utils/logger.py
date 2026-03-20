import logging
import os
import sys
from logging.handlers import RotatingFileHandler

LOG_DIR = "logs" # thư mục chứa log

def get_logger(name, log_filename="pipeline.log", level=logging.INFO):
    
    # tạo thư mục logs nếu chưa tồn tại.
    os.makedirs(LOG_DIR, exist_ok=True)

    # khởi tạo logger
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # chỉ config nếu chưa có handler
    if not logger.hasHandlers():
        logger.setLevel(level)
    
        # định dạng log chuẩn: [Thời gian] [Mức độ] [Tên file] - Nội dung
        formatter = logging.Formatter(
            fmt="[%(asctime)s] [%(levelname)s] [%(name)s] - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        # console
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(level)
        logger.addHandler(console_handler)

        # file
        if log_filename:
            log_filepath = os.path.join(LOG_DIR, log_filename)
            try:
                file_handler = RotatingFileHandler(
                    log_filepath,
                    maxBytes=5 * 1024 * 1024,
                    backupCount=3,
                    encoding="utf-8",
                    errors="replace"
                )
                file_handler.setFormatter(formatter)
                file_handler.setLevel(level)
                logger.addHandler(file_handler)
            except Exception as e:
                logger.warning(f"Không tạo được file log {log_filename}: {e}")

        # ngăn log bị đẩy lên root logger của hệ thống
        logger.propagate = False

    return logger