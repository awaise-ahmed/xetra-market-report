from xetra.common.s3 import S3BucketConnector
from datetime import datetime as dt
from datetime import timedelta as td
from botocore.exceptions import ClientError


class MetaProcess():

    @staticmethod
    def get_meta_details(bucket: S3BucketConnector,key: str):
        try:
            df = bucket.read_csv_to_df((key,None))
            print('Meta file exists')
            print(key)
            return df,df.source_date.max()
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                print('Meta File does not exist!')
                # Handle the NoSuchKey error
                return None,None # Get Yaml date

    @staticmethod
    def get_bucket_objects(bucket: S3BucketConnector,
                           curr_dt: dt,
                           arg_dt: dt,
                           src_dt_fmt: str,
                           col_list: list):
        res_list = []
        for obj in bucket._bucket.objects.all():
            obj_dt = dt.strptime(obj.key.split('/')[0],src_dt_fmt).date()
            if arg_dt - td(days=5) <= obj_dt <= curr_dt:
                res_list.append((obj.key,col_list))
        return res_list # Take 5 days earlier data

    @staticmethod
    def update_meta_file():
        pass
    