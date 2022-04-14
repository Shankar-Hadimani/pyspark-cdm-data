# This module is used for generic transformations that are reused across
from pyspark.sql import DataFrame
from  pyspark.sql import functions as F



def add_surrogate_key(logger, df:DataFrame) -> DataFrame:
    """ADD Surrogate Key column with UUID values

    Args:
        df (DataFrame): DataFrame

    Returns:
        DataFrame: DataFRame with UUID column
    """
    try:
        df = df.withColumn("id", F.expr("uuid()"))
    except Exception as e:
        logger.error("Error while adding surrogate key column to dataframe, : " + str(e))

    return df


def add_timestamp_cols(logger, df:DataFrame) -> DataFrame:
    """Add Start Timestamp and End Timestamp colums to dimension /facts table

    Args:
        df (DataFrame): Input Spark DataFrame

    Returns:
        DataFrame: Spark DataFrame with addtional 2 timestamp columns
    """
    try:
        df = (df.withColumn("effective_start_dtm", F.current_timestamp())
        .withColumn("effective_end_dtm", F.lit(None).cast('timestamp'))
        )
    except Exception as e:
        logger.error("Error while adding timestamp columns to dataframe, : " + str(e))

    return df


def extract_data_from_sql(spark, logger, src_sql_query: str) -> DataFrame:
    try:
        df = spark.sql(""" {query} """.format(query=src_sql_query))
    except Exception as e:
        logger.error("Incorrect SQL SELECT query, : " + str(e))
    
    return df


def create_temp_view(logger, df: DataFrame, temp_table_name: str) -> None:
    try:
        df.createOrReplaceTempView(temp_table_name)
    except Exception as e:
        logger.error("Error while creating TEMP View, : " + str(e))
    
    return None
