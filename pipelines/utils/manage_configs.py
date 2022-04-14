import os
import json
from pyspark.sql import SparkSession

# set spark session
spark = SparkSession.builder.getOrCreate()


def is_local():
    """ method checks and returns true if it's local environment.

    Returns:
        boolean: True / False
    """
    spark = SparkSession.builder.getOrCreate()
    setting = spark.conf.get("spark.master")
    if "local" in setting:
        return True
    else:
        return False


def load_config_file():
    """Load th econfiguration file from config directory,
    within the project directory, if it's local environment.
    otherwise, load config file from DBFS config location
    from cluster.

    Returns:
        Dictionary: JSON dictionary
    """
    if is_local():
        dir_name = os.path.dirname(__file__)
        config_file = os.path.join(
            dir_name, '../../configs/env_config/config.json')
    else:
        # Note this path must match the path in Deploy.ps1 parameter TargetDBFSFolderCode
        config_file = "/dbfs/biopharm/code/configs/env_config/config.json"
    with open(config_file, 'r') as f:
        config = json.load(f)
    return config


def get_config_setting(setting_name):
    """gets configuration settings from config file, based on
    respective environment.

    Args:
        setting_name (String): Setting name from config file

    Returns:
        Dict: JSON dictionary
    """
    config = load_config_file()
    return config['settings'][setting_name]


def load_etl_config(etl_config_file_name: str):
    """Load th econfiguration file from etl_config directory,
    within the project directory, if it's local environment.
    otherwise, load config file from DBFS config location
    from cluster.

    Returns:
        Dictionary: JSON dictionary
    """
    if is_local():
        dir_name = os.path.dirname(__file__)
        config_file = os.path.join(
            dir_name, '../../configs/etl_config/' + etl_config_file_name + '.json')
    else:
        # Note this path must match the path in Deploy.ps1 parameter TargetDBFSFolderCode
        config_file = "/dbfs/biopharm/code/configs/etl_config/config.json" + \
            etl_config_file_name + ".json"
    with open(config_file, 'r') as f:
        config = json.load(f)
    return config


def get_etl_config_setting(etl_config_file_name: str):
    """gets configuration settings from config file, based on
    respective etl configuration files.

    Args:
        setting_name (String): Setting name from config file

    Returns:
        Dict: JSON dictionary
    """
    config = load_etl_config(etl_config_file_name)
    return config['metadata']


def set_database():
    """
    creates database and use the same databse from cluster.
    """
    database_name = get_config_setting("DatabaseName")
    spark.sql(""" CREATE DATABASE IF NOT EXISTS {db_name} """.format(
        db_name=database_name))
    spark.sql(""" USE {db_name} """.format(db_name=database_name))

    return None
