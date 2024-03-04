import os
import boto3
import logging
import pandas as pd

from io import StringIO, BytesIO
from botocore.exceptions import ClientError
from xetra.common.constants import S3FileTypes

class S3BucketConnector():
    def __init__(self, access_key: str,secret_key: str, endpoint_url: str, bucket: str):
        self._logger = logging.getLogger(__name__)
        self.endpoint_url = endpoint_url
        self.session = boto3.Session(aws_access_key_id = os.environ[access_key],
                                     aws_secret_access_key = os.environ[secret_key])
        
        self._s3 = self.session.resource(service_name='s3')
        self._bucket = self._s3.Bucket(bucket)
        self._logger.info('Bucket object created!')

    def read_csv_to_df(self,obj: list, encoding: str = 'utf-8', sep: str = ','):
        self._logger.debug('Inside read_csv_to_df method')
        df_cols = obj[1]

        # try:
        with self._bucket.Object(key=obj[0]).get().get('Body') as file_obj:
            # self._logger.debug('Inside Try')
            csv_obj = file_obj.read().decode(encoding)
            data = StringIO(csv_obj)
            df = pd.read_csv(data,sep=sep)

            if df.shape[0]==0:
                return None
            elif df_cols:
                return df[df_cols]
            else:
                return df


    def write_df_to_s3(self,df: pd.DataFrame, key: str,file_format: str):
        out_buffer = BytesIO()
        if file_format == S3FileTypes.PARQUET.value:
            df.to_parquet(out_buffer,index=False)
        elif file_format == S3FileTypes.CSV.value:
            df.to_csv(out_buffer,index=False)
        
        self._bucket.put_object(Body=out_buffer.getvalue(),Key=key)
        return True
