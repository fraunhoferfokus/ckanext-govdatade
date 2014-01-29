import ConfigParser
import os

# this is a namespace package
try:
    import pkg_resources
    pkg_resources.declare_namespace(__name__)
except ImportError:
    import pkgutil
    __path__ = pkgutil.extend_path(__path__, __name__)


# Make configuration file globally accessible
CONFIG = ConfigParser.ConfigParser()

config_file = os.path.dirname(__file__)
config_file = os.path.join(config_file, '../..', 'config.ini')
config_file = os.path.abspath(config_file)

CONFIG.read(config_file)
