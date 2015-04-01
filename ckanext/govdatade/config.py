import ConfigParser
import os

# Make configuration file globally accessible
CONFIG = ConfigParser.ConfigParser()

config_file = os.path.dirname(__file__)
config_file = os.path.join(config_file, '../..', 'config.ini')
config_file = os.path.abspath(config_file)

CONFIG.read(config_file)
