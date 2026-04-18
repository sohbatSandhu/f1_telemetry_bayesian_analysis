import logging
import os
from datetime import datetime
import sys

# creates logs folder
logs_path = os.path.join(os.getcwd(), "logs")
os.makedirs(logs_path, exist_ok=True)

# creates folder for all the 
LOG_FOLDER= f"{datetime.now().strftime('%m_%d_%Y')}"
logs_folder_path = os.path.join(logs_path, LOG_FOLDER)
os.makedirs(logs_folder_path, exist_ok=True)

LOG_FILE= f"{datetime.now().strftime('%H_%M_%S')}.log"
LOG_FILE_PATH = os.path.join(logs_folder_path, LOG_FILE)

logging.basicConfig(
    filename=LOG_FILE_PATH,
    format= "[%(asctime)s] %(lineno)d %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

# Exception utilities
def error_message_detail(error, error_detail):
    _, _, exe_tb = error_detail.exc_info()
    file_name = exe_tb.tb_frame.f_code.co_filename
    error_message = "Error occured in python script name [{0}] line number [{1}] and error message [{2}]".format(
        file_name, exe_tb.tb_lineno, str(error)
    )
    
    return error_message
    
class CustomException(Exception):
    def __init__(self, error_message, error_detail):
        super().__init__(error_message)
        self.error_message = error_message_detail(error_message, error_detail=error_detail)
    
    def __str__(self):
        return self.error_message

if __name__ == "__main__":
    logging.info("Logging has started")