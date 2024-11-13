import logging
from pathlib import Path
from config import config

vadis_logger = logging.getLogger('vadis_logger')
vadis_logger.setLevel(logging.DEBUG)
fh = logging.FileHandler(Path(__file__).with_name(config['logger']))
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
fh.setFormatter(formatter)
vadis_logger.addHandler(fh)
vadis_logger.propagate = False
