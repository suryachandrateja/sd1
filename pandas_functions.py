import math
from datetime import datetime
from pyspark.sql.functions import *

# Initial Dataset (pyspark dataframe)
opens = spark.sql("select * from bronze_layer_beacons.flipp_open").select("t", "flyer_id", "account_guid", "sid", "date", "time_iso8601")

evs = spark.sql("select * from bronze_layer_beacons.flipp_ev").select("t", "flyer_id", "account_guid", "sid", "date", "time_iso8601")

dataset = opens.unionByName(evs)

# Pandas transformation functions
# Please move to PySpark section if not familiar with Pandas.
# Please only review either the Pandas or PySpark implementation of the code.
def pandasLagTime(df):
  df['prev_time_iso']=df.sort_values(by=['time_iso8601'], ascending=True).groupby(['account_guid'])['time_iso8601'].shift(1)
  return df


def pandasCasting(df):
  df['prev_time_iso']=df["prev_time_iso"].astype('datetime64[s]')
  df['time_iso8601']=df["time_iso8601"].astype('datetime64[s]')
  return df


def orderingPandasDataFrame(df):
   return df.sort_values(by=['account_guid', 'time_iso8601'])


def pandasCleanUserId(df):
  return df[(df['account_guid']!='%3Cnull%3E')]


def pandasTimeDiff(df):
  df['temp_prev_time_iso']=df['prev_time_iso'].combine_first(df['time_iso8601'])
  df['diff']=df['time_iso8601'].subtract(df['temp_prev_time_iso'])/1e9
  df['diff']=df['diff'].astype('int')
  df = df.drop('temp_prev_time_iso', 1)
  return df

def pandasDefineSession(df):
  df.loc[(df['diff'] > 600) | (df['prev_time_iso'].isna()), 'new_session'] = 1
  df.loc[(df['diff'] <= 600) & (df['prev_time_iso'].notna()), 'new_session'] = 0
  return df

# Sample Pandas Output