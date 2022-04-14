import pytest
from .context import pipelines, dependencies
from pipelines.utils import transformations as tfm
from dependencies import run_spark, logging
from pyspark.sql import SparkSession


### set spark session and logger with app_name
# spark_session, spark_logger = run_spark('test_transformation')
spark_session  = SparkSession.builder.appName('test_transformation').getOrCreate()
spark_logger = logging.Log4j(spark=spark_session)


class TestTransformation:
    def mock_extract():
        data = [("A1","B1"), ("A2","B2")]
        return spark_session.createDataFrame(data, ["col1", "col2"])

    def mock_spark_sql():
        mock_sql = """SELECT col1, col2  FROM VALUES (1,'XXX'),(2, 'YYY'); """
        return mock_sql

    def test_add_surrogate_key(self):
        df = TestTransformation.mock_extract()
        df = tfm.add_surrogate_key(spark_logger, df)
        assert "id" in df.columns

    def test_add_timestamp_cols(self):
        df = TestTransformation.mock_extract()
        df = tfm.add_timestamp_cols(spark_logger, df)

        assert "effective_start_dtm" in df.columns
        assert "effective_end_dtm" in df.columns

    def test_extract_data_from_sql(self):
        mock_sql = TestTransformation.mock_spark_sql()
        df = tfm.extract_data_from_sql(
            spark=spark_session, 
            logger=spark_logger, 
            src_sql_query=mock_sql)

        assert "col1" in df.columns
        assert "col2" in df.columns
        assert df.head()[0] == 1
        assert df.head()[1] == 'XXX'
        assert str(df.count()) == '2'

    def test_create_temp_view(self):
        mock_data_df = TestTransformation.mock_extract()
        create_view_df = tfm.create_temp_view(
            logger=spark_logger, 
            df= mock_data_df, 
            temp_table_name='test_temp')
        
        df_count = spark_session.sql(" SELECT * FROM test_temp").count()

        assert create_view_df is None
        assert str(df_count) == '2'






    