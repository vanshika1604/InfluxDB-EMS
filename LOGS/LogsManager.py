import logging
import os
from datetime import datetime
from CFG.ConfigHandler import ConfigHandler

class Log:
    def __init__(self):
        # Initialise logging level and log location
        config_handler = ConfigHandler()
        self.configdata = config_handler.config_read()
        self.DateString = "%Y-%m-%d"
        self.TimeString = "%H-%M-%S"
        self.date = str(datetime.now().strftime(self.DateString))
        self.time = str(datetime.now().strftime(self.TimeString))
        self.dir_name = str(os.environ.get('LOG_DIR',self.configdata['Log']['log_dir']) + self.date)
        if not os.path.exists(self.dir_name):
            os.makedirs(self.dir_name)
        logging.basicConfig(level=int(os.environ.get('LOG_LEVEL',self.configdata['Log']['log_level'])), format='%(asctime)s %(levelname)-8s %(message)s ',
                            datefmt='%a, %d %b %Y %H:%M:%S', filename=self.dir_name + '/' + self.time + '.log',
                            filemode='w')
        self.logger = logging

    def DEBUG(self, msg):
        self.logger.debug(msg)

    def ERROR(self, msg):
        self.logger.error(msg)

    def WARNING(self, msg):
        self.logger.warning(msg)

    def INFO(self, msg):
        self.logger.info(msg)
