import importlib
from pyspark.sql import SparkSession
from importlib import import_module
from inspect import signature
from dependencies import logging
import os
import sys
import argparse



# create spark session and attach spark logger object
spark = SparkSession.builder.getOrCreate()

# Also adding the modules hint so that we can execute this script locally
try:
    dir_name = os.path.dirname(__file__)
    sys.path.insert(0,(os.path.join(dir_name, 'pipelines')))
except:
    print('nothing')

# define the parameters for the job
parser = argparse.ArgumentParser()

parser.add_argument("etl", type=str,nargs='?', default='src.jobs.amazon')
namespace, extra = parser.parse_known_args()
for arg in vars(namespace):
    etl = getattr(namespace, arg)

# import module based on first parameter passed
mod = import_module(etl, "src")
met = getattr(mod, "etl")


# Get the parameters (if any) for the ETL
p = signature(met)
for a, b in p.parameters.items():
    parser.add_argument(b.name, type=str, nargs='?')

args = parser.parse_args()

# Loop through the arguements and pass to ETL (try casting to int)
l = []
for arg in vars(args):
    print(arg, getattr(args, arg))
    if arg != "etl":
        try:
            l.append(int(getattr(args, arg)))
        except:
            l.append(getattr(args, arg))

# execute ETL
met(*l)