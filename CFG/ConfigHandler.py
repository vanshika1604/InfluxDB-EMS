import os
import json

class ConfigHandler:

    def __init__(self):

        self.filename = "CFG/StaticConfig.json"

    # Function to read config file
    def config_read(self):
        try:
            with open(self.filename, "r") as jsonfile:
                configdata = json.load(jsonfile)
            return configdata
        except Exception as ex:
            print("\nStatic Config Read Failed " + str(os.path.basename(__file__)) + str(ex))
        return None

    # Function to update data in config file
    def config_write(self, key, value):
        try:
            with open(self.filename, "r") as jsonfile:
                configdata = json.load(jsonfile)
                configdata[key] = value
            with open(self.filename, "w") as jsonfile:
                json.dump(configdata, jsonfile)
        except Exception as ex:
            print("\nStatic Config Write Failed " + str(os.path.basename(__file__)) + str(ex))
        return None