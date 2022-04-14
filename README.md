# pyspark-CDM-data

## Setup

Create a Conda Environment (open Conda prompt):
```
conda create --name dbconnectappdemo python=3.8
```

Activate the environment:
```
conda activate dbctalend
```

IMPORTANT: Open the requirements.txt in the root folder and ensure the version of databrick-connect matches your cluster runtime.

Install the requirements into your environments:
```
pip install -r requirements.txt
```

If you need to setup databricks-connect then run:
```
databricks-connect configure
```
