# ETL with a parameter - see etl() function
from pipelines.utils import manage_configs as cm
from pipelines.utils import transformations as tfm
from dependencies.run_spark import run_spark


def etl(config_name: str):
    """ execute SCD type 2 process

    Args:
        config_name (str): ETL metadata config name
    """
    app_name = "app_" + config_name
    spark, logger = run_spark(app_name)
    try:
        etl_config = cm.get_etl_config_setting(config_name)
        load_type = etl_config['load_strategy']
        etl_in_cfg = etl_config['input']['dbx_hive']
        etl_out_cfg = etl_config['output']['dbx_hive']
        etl_merge_cfg = etl_config['merge']['dbx_hive']
        if load_type:
            try:
                if load_type.strip().upper() == 'SCD2':
                    if etl_in_cfg['src_query_exists']:
                        # read source SQL query
                        src_df = tfm.extract_data_from_sql(
                            spark, logger,
                            etl_in_cfg['src_sql']
                            )

                        # add surrogate key
                        src_df = tfm.add_surrogate_key(
                            logger=logger,
                            df=src_df)
                        
                        # add effective start and end timestamps
                        src_df = tfm.add_timestamp_cols(
                            logger=logger,
                            df=src_df)

                        # create temp view for Source
                        tfm.create_temp_view(
                            logger=logger, 
                            df=src_df,
                            temp_table_name=etl_in_cfg['src_query_alias'])
                        

                    if etl_out_cfg['dest_query_exists']:
                        # read from destination SQL query
                        tgt_df = tfm.extract_data_from_sql(
                            spark, logger,
                            etl_out_cfg['dest_sql']
                            )
                        # create temp view for Target
                        tfm.create_temp_view(
                            logger=logger,df=tgt_df, 
                            temp_table_name=etl_out_cfg['dest_query_alias'])

                    if etl_merge_cfg['merge_query_exists']:

                        # log that main ETL job is starting and execute the MERGE query
                        logger.warn('Job is up-and-running for configuration :=> ' + config_name)
                        spark.sql(etl_merge_cfg['merge_sql'])
                        logger.warn('Job is finsihed successfully, for configuration :=> ' + config_name)
            except Exception as e:
                logger.error('Job failed, while using loading strategy {load_type}, for dimension/fact - {config}., with error {err}'.format(load_type=load_type,
                                                                                                                          config=config_name, err=str(e)))
        else:
            logger.error("Incorrect loading strategy {load_type} value in project config file {config}.json:".format(load_type=load_type,
                                                                                                                     config=config_name))
    except Exception as e:
        logger.error("Cannot find config name in argument variables/config json : " + str(e))
