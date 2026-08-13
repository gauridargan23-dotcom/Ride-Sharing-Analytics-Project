from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, FloatType

BASE_PATH = Path(".").resolve()
DATA_PATH = BASE_PATH / "data"
OUTPUT_PATH = BASE_PATH / "output"
BRONZE_PATH, SILVER_PATH, GOLD_PATH = OUTPUT_PATH/"bronze", OUTPUT_PATH/"silver", OUTPUT_PATH/"gold"
for p in [BRONZE_PATH, SILVER_PATH, GOLD_PATH]: p.mkdir(parents=True, exist_ok=True)

spark = (SparkSession.builder.appName("RideSharingDriverPerformance")
         .config("spark.sql.adaptive.enabled","true").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

def read_csv(name):
    return spark.read.option("header",True).option("inferSchema",True).csv(str(DATA_PATH/name))

drivers_raw, trips_raw, logs_raw = read_csv("drivers.csv"), read_csv("trips.csv"), read_csv("trip_logs.csv")

# Bronze: raw data as-is
drivers_raw.coalesce(1).write.mode("overwrite").parquet(str(BRONZE_PATH/"drivers"))
trips_raw.coalesce(1).write.mode("overwrite").parquet(str(BRONZE_PATH/"trips"))
logs_raw.coalesce(1).write.mode("overwrite").parquet(str(BRONZE_PATH/"trip_logs"))
# Silver: clean and validate
drivers = (drivers_raw.select("driver_id","name","city","rating").dropDuplicates(["driver_id"])
           .withColumn("driver_id",F.col("driver_id").cast(IntegerType()))
           .withColumn("rating",F.col("rating").cast(FloatType()))
           .filter(F.col("driver_id").isNotNull()).filter(F.col("rating").between(0,5))
           .na.fill({"city":"Unknown","rating":0.0}))

trips = (trips_raw.select("trip_id","driver_id","pickup_location","drop_location","distance_km","fare_amount","trip_status")
         .dropDuplicates(["trip_id"])
         .withColumn("trip_id",F.col("trip_id").cast(IntegerType()))
         .withColumn("driver_id",F.col("driver_id").cast(IntegerType()))
         .withColumn("distance_km",F.col("distance_km").cast(FloatType()))
         .withColumn("fare_amount",F.col("fare_amount").cast(FloatType()))
         .withColumn("trip_status",F.trim("trip_status"))
         .filter(F.col("trip_id").isNotNull()).filter(F.col("driver_id").isNotNull())
         .filter(F.col("distance_km")>=0).filter(F.col("fare_amount")>=0)
         .filter(F.col("trip_status").isin("Completed","Cancelled")))

logs = (logs_raw.select("log_id","trip_id","start_time","end_time","delay_minutes","cancellation_flag")
        .dropDuplicates(["log_id"])
        .withColumn("log_id",F.col("log_id").cast(IntegerType()))
        .withColumn("trip_id",F.col("trip_id").cast(IntegerType()))
        .withColumn("start_time",F.to_timestamp("start_time"))
        .withColumn("end_time",F.to_timestamp("end_time"))
        .withColumn("delay_minutes",F.col("delay_minutes").cast(FloatType()))
        .withColumn("cancellation_flag",F.col("cancellation_flag").cast(IntegerType()))
        .filter(F.col("trip_id").isNotNull()).filter(F.col("delay_minutes")>=0)
        .filter(F.col("cancellation_flag").isin(0,1)).filter(F.col("start_time").isNotNull()))

silver = (trips.join(logs,"trip_id","inner").join(F.broadcast(drivers),"driver_id","left")
          .withColumn("completion_flag",F.when(F.col("trip_status")=="Completed",1).otherwise(0))
          .withColumn("cancelled_flag",F.when((F.col("trip_status")=="Cancelled")|(F.col("cancellation_flag")==1),1).otherwise(0))
          .withColumn("trip_duration_minutes",
                      F.when(F.col("start_time").isNotNull()&F.col("end_time").isNotNull(),
                             (F.col("end_time").cast("long")-F.col("start_time").cast("long"))/60.0))
          .withColumn("revenue",F.when(F.col("trip_status")=="Completed",F.col("fare_amount")).otherwise(0.0))
          .withColumn("delay_flag",F.when(F.col("delay_minutes")>0,1).otherwise(0))
          # cancelled trips may legitimately have no end_time; completed trips may not
          .filter((F.col("trip_status")=="Cancelled")|F.col("end_time").isNotNull())
          .filter(F.col("trip_duration_minutes").isNull()| (F.col("trip_duration_minutes")>=0)))
silver.cache()
silver.coalesce(1).write.mode("overwrite").parquet(
    str(SILVER_PATH/"trips_enriched")
)   

overall = silver.agg(
    F.count("*").alias("total_trips"), F.sum("completion_flag").alias("completed_trips"),
    F.sum("cancelled_flag").alias("cancelled_trips"), F.round(F.sum("revenue"),2).alias("total_revenue"),
    F.round(F.avg(F.when(F.col("completion_flag")==1,F.col("fare_amount"))),2).alias("avg_completed_fare"),
    F.round(F.avg("delay_minutes"),2).alias("avg_delay_minutes"),
    F.round(F.avg(F.when(F.col("completion_flag")==1,F.col("trip_duration_minutes"))),2).alias("avg_completed_duration_minutes")
).withColumn("completion_rate_pct",F.round(F.col("completed_trips")/F.col("total_trips")*100,2))\
 .withColumn("cancellation_rate_pct",F.round(F.col("cancelled_trips")/F.col("total_trips")*100,2))
overall.coalesce(1).write.mode("overwrite").parquet(
    str(GOLD_PATH/"overall_kpi")
)

driver = silver.groupBy("driver_id","name","city","rating").agg(
    F.count("*").alias("total_trips"),F.sum("completion_flag").alias("completed_trips"),
    F.sum("cancelled_flag").alias("cancelled_trips"),F.round(F.sum("revenue"),2).alias("total_revenue"),
    F.round(F.avg(F.when(F.col("completion_flag")==1,F.col("fare_amount"))),2).alias("avg_completed_fare"),
    F.round(F.avg("delay_minutes"),2).alias("avg_delay_minutes"),
    F.round(F.avg(F.when(F.col("completion_flag")==1,F.col("trip_duration_minutes"))),2).alias("avg_trip_duration_minutes")
).withColumn("completion_rate_pct",F.round(F.col("completed_trips")/F.col("total_trips")*100,2))\
 .withColumn("cancellation_rate_pct",F.round(F.col("cancelled_trips")/F.col("total_trips")*100,2))
w=Window.orderBy(F.col("completion_rate_pct").desc(),F.col("cancellation_rate_pct").asc(),F.col("rating").desc(),F.col("total_revenue").desc())
driver=driver.withColumn("performance_rank",F.row_number().over(w))
driver.coalesce(1).write.mode("overwrite").parquet(
    str(GOLD_PATH/"driver_performance")
)

pickup=silver.groupBy("pickup_location").agg(
    F.count("*").alias("total_trips"),F.sum("completion_flag").alias("completed_trips"),
    F.sum("cancelled_flag").alias("cancelled_trips"),F.round(F.sum("revenue"),2).alias("revenue"),
    F.round(F.avg("delay_minutes"),2).alias("avg_delay_minutes")
).withColumn("completion_rate_pct",F.round(F.col("completed_trips")/F.col("total_trips")*100,2))
pickup.coalesce(1).write.mode("overwrite").parquet(
    str(GOLD_PATH/"pickup_demand")
)

delay=silver.groupBy("city").agg(
    F.count("*").alias("total_trips"),F.round(F.avg("delay_minutes"),2).alias("avg_delay_minutes"),
    F.max("delay_minutes").alias("max_delay_minutes"),F.sum("delay_flag").alias("delayed_trips")
).withColumn("delay_rate_pct",F.round(F.col("delayed_trips")/F.col("total_trips")*100,2))
delay.coalesce(1).write.mode("overwrite").parquet(
    str(GOLD_PATH/"delay_analysis")
)

revenue=silver.groupBy("city").agg(
    F.count("*").alias("total_trips"),F.sum("completion_flag").alias("completed_trips"),
    F.round(F.sum("revenue"),2).alias("total_revenue"),
    F.round(F.avg(F.when(F.col("completion_flag")==1,F.col("fare_amount"))),2).alias("avg_completed_fare")
)
revenue.coalesce(1).write.mode("overwrite").parquet(
    str(GOLD_PATH/"revenue_by_city")
)

cancel=silver.groupBy("driver_id","name","city").agg(
    F.count("*").alias("total_trips"),F.sum("cancelled_flag").alias("cancelled_trips")
).withColumn("cancellation_rate_pct",F.round(F.col("cancelled_trips")/F.col("total_trips")*100,2))
cancel.coalesce(1).write.mode("overwrite").parquet(
    str(GOLD_PATH/"cancellation_by_driver")
)

print("Pipeline completed successfully.")
print("Total trips:", silver.count())
spark.stop()
