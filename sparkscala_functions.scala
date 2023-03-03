// Initial Dataset (dataframe)
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import java.time.LocalDate 
import org.apache.spark.sql.expressions.Window
var opens = spark.sql("select * from bronze_layer_beacons.flipp_open").filter('date > "2022-01-01").select("t", "flyer_id", "account_guid", "sid", "date", "time_iso8601")

var evs = spark.sql("select * from bronze_layer_beacons.flipp_ev").filter('date > "2022-01-01").select("t", "flyer_id", "account_guid", "sid", "date", "time_iso8601")

var dataset = opens.unionByName(evs)

//Spark Scala transformation functions

def scalaSparkLagTime(df: DataFrame) ={
   val w= Window.partitionBy("account_guid").orderBy(col("time_iso8601").asc_nulls_first)
   df.withColumn("prev_time_iso", lag("time_iso8601",1,0).over(w))
  }

 def scalaSparkCleanUserId(df: DataFrame)={
    df.filter(col("account_guid")=!="%3Cnull%3E")
}

 def scalaSparkTimeDiff(df: DataFrame) = {
    df.withColumn("temp_prev_time_iso", coalesce(col("prev_time_iso"), col("time_iso8601"))) 
     .withColumn("temp_prev_time_iso", to_timestamp(col("temp_prev_time_iso"), "yyyy-MM-dd'T'HH:mm:ssXXXXX")) 
     .withColumn("curr_time_iso8601", to_timestamp(col("time_iso8601"), "yyyy-MM-dd'T'HH:mm:ssXXXXX")) 
     .withColumn("diff", ((col("curr_time_iso8601").cast("long") - col("temp_prev_time_iso").cast("long"))).cast("int"))
}

def scalaSparkDefineSession(df: DataFrame)= {
  val w= Window.partitionBy("account_guid").orderBy(col("time_iso8601").asc_nulls_first)
   df.withColumn("new_session_flag", when((col("prev_time_iso").isNull) or (col("diff") >= 600), lit(1)).otherwise(lit(0))) 
           .withColumn("session_id", sum("new_session_flag").over(w))
  }

 def scalaSparkStartEndSessionTimes(df: DataFrame)={
   val w=Window.partitionBy("account_guid", "session_id").orderBy(col("time_iso8601").asc_nulls_first).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
   df.withColumn("start_time", first("time_iso8601").over(w)).withColumn("end_time", last("time_iso8601").over(w))
}
  def scalaSparkSessionDuration(df: DataFrame)={
    val w= Window.partitionBy("account_guid", "session_id").orderBy(col("time_iso8601").asc_nulls_first).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    df.withColumn("session_duration", sum("diff").over(w))
 }

// Sample Output (calling above functions)
val scalaSparkTransform=dataset 
     .transform(scalaSparkLagTime) 
     .transform(scalaSparkCleanUserId) 
     .transform(scalaSparkTimeDiff) 
     .transform(scalaSparkDefineSession) 
     .transform(scalaSparkStartEndSessionTimes) 
     .transform(scalaSparkSessionDuration) 
     .select("account_guid", "start_time", "end_time", "session_duration", "session_id")

display(scalaSparkTransform)