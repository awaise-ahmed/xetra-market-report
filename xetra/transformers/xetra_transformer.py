from typing import NamedTuple
from datetime import datetime as dt
from concurrent.futures import ThreadPoolExecutor
from xetra.common.s3 import S3BucketConnector
from xetra.common.meta_process import MetaProcess
from xetra.common.constants import S3FileTypes

import logging
import pandas as pd


class XetraSourceConfig(NamedTuple):
    src_first_extract_date: str
    src_parallel_workers: int
    src_date_format: str
    src_columns: list
    src_col_date: str
    src_col_isin: str
    src_col_time: str
    src_col_start_price: str
    src_col_end_price: str
    src_col_min_price: str
    src_col_max_price:str
    src_col_traded_vol: str

class XetraTargetConfig(NamedTuple):
    trg_col_isin: str
    trg_col_date: str
    trg_col_open_price: str
    trg_col_close_price: str
    trg_col_max_price: str
    trg_col_min_price: str
    trg_col_daily_traded_vol: str
    trg_col_chg_prev_close: str
    trg_key: str
    trg_key_date_format: str
    trg_format: str

class XetraETL():
    def __init__(self, 
                 s3_bucket_src: S3BucketConnector,
                 s3_bucket_trg: S3BucketConnector, 
                 meta_key: str,
                 src_args: XetraSourceConfig, 
                 trg_args: XetraTargetConfig):

        self.curr_date = '2022-04-20'

        self._logger = logging.getLogger(__name__)
        self._logger.info("Initializing XetraETL..")
        
        self.s3_bucket_src = s3_bucket_src
        self.s3_bucket_trg = s3_bucket_trg
        self.meta_key = meta_key
        self.src_args = src_args
        self.trg_args = trg_args

        self.meta_df, self.arg_date = MetaProcess.get_meta_details(self.s3_bucket_trg,self.meta_key)
        if not self.arg_date:
            self.arg_date = self.src_args.src_first_extract_date

        arg_date_dt = dt.strptime(self.arg_date,self.src_args.src_date_format).date()
        curr_date_dt = dt.strptime(self.curr_date,self.src_args.src_date_format).date()

        self._logger.debug("arg_date: {}".format(self.arg_date))
        self._logger.debug("arg_date_dt: {}".format(arg_date_dt))
        self._logger.debug("curr_date_dt: {}".format(curr_date_dt))

        self.obj_list = MetaProcess.get_bucket_objects(self.s3_bucket_src,curr_date_dt,arg_date_dt,self.src_args.src_date_format,self.src_args.src_columns)
        for x in self.obj_list:
            self._logger.debug("obj_list: {}".format(x))


    def extract(self):
        with ThreadPoolExecutor(max_workers=self.src_args.src_parallel_workers) as executor:
            results = list(executor.map(self.s3_bucket_src.read_csv_to_df, self.obj_list))

        for res in results:
            self._logger.debug(res)
        if results:
            df = pd.concat(results, ignore_index=True)
            self._logger.debug('Unique Date list : {}'.format(df.Date.unique()))
            return df
        else:
            return None

    def transform_report1(self,df: pd.DataFrame):
        if df.empty:
            self._logger.info('The dataframe is empty. No transformations will be applied.')
            return df
        self._logger.info('Applying transformations to Xetra source data for report 1 started...')

        df = df.loc[:, self.src_args.src_columns]
        df.dropna(inplace=True)


        df.sort_values(by=[self.src_args.src_col_date,self.src_args.src_col_isin],inplace=True)
        df['opening_price'] = df.sort_values(by='Time').groupby(['Date','ISIN'])['StartPrice'].transform('first')
        df['closing_price'] = df.sort_values(by='Time').groupby(['Date','ISIN'])['StartPrice'].transform('last')

        df_agg = df.groupby(['Date','ISIN'], as_index=False).agg(
            opening_price=('opening_price','min'), 
            closing_price=('closing_price','min'), 
            minimum_price=('MinPrice','min'), 
            maximum_price=('MaxPrice','min'), 
            daily_traded_vol=('TradedVolume','sum')
        )
        df_agg['prev_closing_price'] = df_agg.sort_values(by='Date').groupby(['ISIN'])['closing_price'].shift(1)
        df_agg['chg_prev_close%'] = (df_agg['prev_closing_price']-df_agg['closing_price'])*100/df_agg['prev_closing_price']

        df_agg.drop(columns=['prev_closing_price'],inplace=True)
        df_agg = df_agg[(df_agg.Date>self.arg_date) & (df_agg.Date<=self.curr_date)]
        df_agg['chg_prev_close%'] = round(df_agg['chg_prev_close%'],2)

        self._logger.info('Applying transformations to Xetra source data finished...')
        return df_agg

    def load(self,df):
        self._logger.info('Loading Xetra report to target started...')
        # print(df)
        date_list = list(df.Date.unique())
        
        for date in date_list:
            key = '{}{}.{}'.format(self.trg_args.trg_key,date,self.trg_args.trg_format)
            self._logger.debug(key)
            self.s3_bucket_trg.write_df_to_s3(df[df.Date==date],key,S3FileTypes.PARQUET.value)

        meta_rec = pd.DataFrame({
                        'source_date':[self.arg_date],
                        'load_timestamp':[dt.now()]
                        })
        if self.meta_df is not None:
            self.meta_df = pd.concat([self.meta_df,meta_rec], ignore_index=True)
        else:
            self.meta_df = pd.DataFrame(meta_rec)

        self.s3_bucket_trg.write_df_to_s3(self.meta_df,self.meta_key,S3FileTypes.CSV.value)
        self._logger.info('Loading Xetra report to target finished...')
        return True



    def etl_report1(self):
        # print('checkpoint 3')
        data_frame = self.extract()
        # print('checkpoint 4')
        data_frame = self.transform_report1(data_frame)
        # print('checkpoint 5')
        # print(df)
        self.load(data_frame)
        # print('checkpoint 6')
        return True
