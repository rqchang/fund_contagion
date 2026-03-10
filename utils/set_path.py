import os
import socket

_user = os.getenv("USERNAME") or os.getenv("USER") or socket.gethostname()

if _user == "chang.2590":
    DIR      = "D:/Dropbox/project/fund_contagion/"
    DATA_DIR = "D:/Dropbox/project/fund_contagion/data"
    RAW_DIR  = "D:/Dropbox/project/fund_contagion/data/raw/"
    TEMP_DIR = "D:/Dropbox/project/fund_contagion/data/temp/"
    PROC_DIR = "D:/Dropbox/project/fund_contagion/data/processed/"
    OUT_DIR  = "D:/Dropbox/project/fund_contagion/outputs/"
    PLOTS_DIR  = "D:/Dropbox/project/fund_contagion/outputs/plots/"
    TABLES_DIR = "D:/Dropbox/project/fund_contagion/outputs/tables/"

elif _user == "User":
    DIR      = "F:/Dropbox (Personal)/project/fund_contagion/"
    DATA_DIR = "F:/Dropbox (Personal)/project/fund_contagion/data"
    RAW_DIR  = "F:/Dropbox (Personal)/project/fund_contagion/data/raw/"
    TEMP_DIR = "F:/Dropbox (Personal)/project/fund_contagion/data/temp/"
    PROC_DIR = "F:/Dropbox (Personal)/project/fund_contagion/data/processed/"
    OUT_DIR  = "F:/Dropbox (Personal)/project/fund_contagion/outputs/"
    PLOTS_DIR  = "F:/Dropbox (Personal)/project/fund_contagion/outputs/plots/"
    TABLES_DIR = "F:/Dropbox (Personal)/project/fund_contagion/outputs/tables/"

else:
    raise EnvironmentError(f"Unknown user '{_user}': add your paths to utils/set_path.py")
