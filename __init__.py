import os, sys

###############################################
# load the project directory to python path

download_scripts_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

if download_scripts_path not in sys.path:
    sys.path.append(download_scripts_path)
