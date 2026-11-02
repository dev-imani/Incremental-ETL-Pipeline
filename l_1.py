# import json 

# file_path = r'C:\Users\Michael\Documents\day3-incremental-etl\incoming\batch_2026-10-21 (1).json'

# with open(file_path,mode="r", encoding ="utf-8") as file:
#     passed_data = json.load(file)
    
# json_string = json.dumps(passed_data,indent=3)
# # dict_ = dict(json_string)

# # print(type(json_string))
# # print(data)
# print(json_string["batch_id"])
# # print(dict_)

from pathlib import Path
import logging
 
 
# WAREHOUSE_PATH = Path("data") / "transactions.csv"
# print(WAREHOUSE_PATH)

LOG_PATH = Path("logs") / "etl.log"

def setup_logging(path:Path=LOG_PATH)-> logging.Logger:
    """Configure a logger that writes to both console and log file"""
    path.parent.mkdir(parents=True,exist_ok=True)
    logger = logging.getLogger("Incremental_etl")
    logger.setLevel(logging.INFo)
    logger.propagate = False
    
    if logger.handles:
        return logger
    
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    file_handler = logging.FileHandler(path,encoding="utf-8")
    file_handler.setFormatter(formatter)
    
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger

print(setup_logging())