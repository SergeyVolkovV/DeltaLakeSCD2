from delta import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import  current_date, sha2, concat_ws
from delta.tables import DeltaTable
from pyspark.sql.types import *

builder = SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = spark = configure_spark_with_delta_pip(builder).getOrCreate()

table_dir = "/home/sv/PP/delta/event"

DeltaTable.createIfNotExists(spark) \
    .tableName("event") \
    .addColumn("date", DateType())\
    .addColumn("eventId", IntegerType())\
    .addColumn("eventType", StringType())\
    .addColumn("data", StringType())\
    .addColumn("valid_from",DateType())\
    .addColumn("valid_to",DateType())\
    .addColumn("current",BooleanType())\
    .addColumn("bk_hash",StringType())\
    .addColumn("row_hash",StringType())\
    .partitionedBy("current")\
    .location(table_dir) \
    .execute()

df = spark.read.format("delta").load(table_dir)

deltaTable = DeltaTable.forPath(spark, table_dir)


schema = StructType([ \
    StructField("date",DateType(),True), \
    StructField("eventId",IntegerType(),True), \
    StructField("eventType",StringType(),True), \
    StructField("data",StringType(),True), \
])

updatesDF = spark.read.load("../event.csv", format="csv", schema=schema, sep=";", inferSchema="true",header="true", dateFormat="MM-dd-yyyy")
#добавляем хеши
updatesDF=updatesDF.withColumn("row_hash", sha2(concat_ws("||", *[  "date"  ,  "eventId"  ,  "eventType"  ,  "data"]), 256)).withColumn("bk_hash", sha2(concat_ws("||", *[    "eventId",      ]), 256))


#определяем новые записи только на вставку
newEvent = updatesDF \
  .alias("updates") \
  .join(df.alias("event"), "bk_hash") \
  .where("event.current = true AND updates.row_hash <> event.row_hash")

#определяем записи удаленные
#delete
delEvent = df \
  .alias("event") \
  .join(updatesDF.alias("updates"), "bk_hash","left") \
  .where("event.current = true and updates.bk_hash is null")

#формируем набор записей для merge
stagedUpdates = (
  newEvent
  .selectExpr("NULL as mergeKey",   "updates.date",  "updates.eventId",  "updates.eventType",  "updates.data","updates.bk_hash","updates.row_hash")   # Rows for new version
  .union(delEvent.selectExpr("bk_hash as mergeKey", "event.date", "event.eventId", "event.eventType", "event.data","event.bk_hash","event.row_hash"))  # Rows for disactive deleted
  .union(updatesDF.alias("updates").selectExpr("updates.bk_hash as mergeKey",  "updates.date",  "updates.eventId",  "updates.eventType",  "updates.data","updates.bk_hash","updates.row_hash"))  # Rows for disactive old version
)

#осуществляем мердже
deltaTable.alias("event").merge(
  stagedUpdates.alias("staged_updates"),
  "event.bk_hash = mergeKey") \
.whenMatchedUpdate(
  condition = "event.current = true AND event.row_hash <> staged_updates.row_hash",
  set = {
    "current": "false",
    "valid_to": current_date()
  }
).whenNotMatchedInsert(
  values = {
    "date":"staged_updates.date"  ,
    "eventId":"staged_updates.eventId"  ,
    "eventType":"staged_updates.eventType"  ,
    "data":"staged_updates.data"  ,
    
    "bk_hash":"staged_updates.bk_hash",
    "row_hash": "staged_updates.row_hash",
    "current": "true",
    "valid_from": current_date(),
    "valid_to": "null"
  }
).execute()

print("############ result ###############")
deltaTable.toDF().orderBy("eventId","valid_from").show(truncate=False)