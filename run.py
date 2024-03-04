import logging
import logging.config
import yaml
import os
from xetra.common.s3 import S3BucketConnector
from xetra.transformers.xetra_transformer import XetraETL, XetraSourceConfig, XetraTargetConfig


def main():
    config_path = os.path.join(os.getcwd(),'config','xetra_report1_config.yml')
    config = yaml.safe_load(open(config_path))
    
    # configure logging
    log_config = config['logging']
    logging.config.dictConfig(log_config)

    # S3 config
    s3_config = config['s3']
    s3_bucket_src = S3BucketConnector(access_key=s3_config['access_key'],
                                      secret_key=s3_config['secret_key'],
                                      endpoint_url=s3_config['src_endpoint_url'],
                                      bucket=s3_config['src_bucket'])

    s3_bucket_trg = S3BucketConnector(access_key=s3_config['access_key'],
                                      secret_key=s3_config['secret_key'],
                                      endpoint_url=s3_config['trg_endpoint_url'],
                                      bucket=s3_config['trg_bucket'])

    # Source/Target/Meta Config
    source_config = XetraSourceConfig(**config['source'])
    target_config = XetraTargetConfig(**config['target'])
    meta_config = config['meta']
    
    logger = logging.getLogger(__name__)
    logger.info("This is the beginning of the ETL pipeline..")
    logger.info("Config path : {}".format(config_path))

    xetra_etl = XetraETL(s3_bucket_src, s3_bucket_trg,
                         meta_config['meta_key'], source_config, target_config)
    xetra_etl.etl_report1()

    logger.info('Xetra ETL job finished!')

if __name__ == '__main__':
    main()