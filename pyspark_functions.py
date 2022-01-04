import math
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import when

# Initial Dataset (pyspark dataframe)
opens = spark.sql("select * from bronze_layer_beacons.flipp_open").select("t", "flyer_id", "account_guid", "sid", "date", "time_iso8601")

evs = spark.sql("select * from bronze_layer_beacons.flipp_ev").select("t", "flyer_id", "account_guid", "sid", "date", "time_iso8601")

dataset = opens.unionByName(evs)

# PySpark transformation functions
# Please move to Pandas section if not familiar with PySpark.
# Please only review either the Pandas or PySpark implementation of the code.
def pySparkLagTime(df):
  w=Window().partitionBy("account_guid").orderBy(col("time_iso8601").asc_nulls_first())
  return df.withColumn("prev_time_iso", lag("time_iso8601").over(w))

def pySparkCleanUserId(df):
  return df.filter(col("account_guid")!="%3Cnull%3E")

def pySparkTimeDiff(df):
  return df.withColumn("temp_prev_time_iso", coalesce(col("prev_time_iso"), col("time_iso8601"))) \
    .withColumn("temp_prev_time_iso", to_timestamp(col("temp_prev_time_iso"), "yyyy-MM-dd'T'HH:mm:ssXXXXX")) \
    .withColumn("curr_time_iso8601", to_timestamp(col("time_iso8601"), "yyyy-MM-dd'T'HH:mm:ssXXXXX")) \
    .withColumn("diff", ((col("curr_time_iso8601").cast("long") - col("temp_prev_time_iso").cast("long"))).cast("int"))

def pySparkDefineSession(df):
  w=Window().partitionBy("account_guid").orderBy(col("time_iso8601").asc_nulls_first())
  return df.withColumn("new_session_flag", when((col("prev_time_iso").isNull()) | (col("diff") >= 600), lit(1)).otherwise(lit(0))) \
           .withColumn("session_id", sum("new_session_flag").over(w))

def pySparkStartEndSessionTimes(df):
  w=Window().partitionBy("account_guid", "session_id").orderBy(col("time_iso8601").asc_nulls_first()).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
  return df.withColumn("start_time", first("time_iso8601").over(w)).withColumn("end_time", last("time_iso8601").over(w))

def pySparkSessionDuration(df):
  w=Window().partitionBy("account_guid", "session_id").orderBy(col("time_iso8601").asc_nulls_first()).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
  return df.withColumn("session_duration", sum("diff").over(w))


# Sample PySpark Output (calling above PySpark functions)
pysparkTransform=dataset \
    .transform(pySparkLagTime) \
    .transform(pySparkCleanUserId) \
    .transform(pySparkTimeDiff) \
    .transform(pySparkDefineSession) \
    .transform(pySparkStartEndSessionTimes) \
    .transform(pySparkSessionDuration) \
    .select("account_guid", "start_time", "end_time", "session_duration", "session_id")