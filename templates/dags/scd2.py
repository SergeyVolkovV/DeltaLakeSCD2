from delta import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import  current_date, sha2, concat_ws
from delta.tables import DeltaTable
from pyspark.sql.types import *

builder = SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = spark = configure_spark_with_delta_pip(builder).getOrCreate()

table_dir = "/home/sv/PP/delta/{{ mapping.target[0].table }}"

DeltaTable.createIfNotExists(spark) \
    .tableName("{{ mapping.target[0].table }}") \{% for mapping in mapping.mapping_ods|sort(attribute='serial') %}
    .addColumn("{{ mapping.target }}", {{ mapping.type }}())\{% endfor %}
    .addColumn("valid_from",DateType())\
    .addColumn("valid_to",DateType())\
    .addColumn("current",BooleanType())\
    .addColumn("bk_hash",StringType())\
    .addColumn("row_hash",StringType())\{% if mapping.target[0].partition!="" %}
    .partitionedBy("{{ mapping.target[0].partition }}")\{% endif %}
    .location(table_dir) \
    .execute()

df = spark.read.format("delta").load(table_dir)

deltaTable = DeltaTable.forPath(spark, table_dir)


schema = StructType([ \{% for mapping in mapping.mapping_ods|sort(attribute='serial') %}
    StructField("{{ mapping.target }}",{{ mapping.type }}(),True), \{% endfor %}
])

updatesDF = spark.read.load("{{ mapping.source[0].table }}", format="csv", schema=schema, sep=";", inferSchema="true",header="true", dateFormat="MM-dd-yyyy")
#добавляем хеши
updatesDF=updatesDF.withColumn("row_hash", sha2(concat_ws("||", *[{% for mapping in mapping.mapping_ods|sort(attribute='serial') %} {%if mapping.serial!="1"%} , {%endif%} "{{ mapping.source[0].column }}"{% endfor %}]), 256)).withColumn("bk_hash", sha2(concat_ws("||", *[{% for mapping in mapping.mapping_ods|sort(attribute='serial') %} {%if mapping.bk=="true"%} "{{ mapping.source[0].column }}", {%endif%} {% endfor %}]), 256))


#определяем новые записи только на вставку
newEvent = updatesDF \
  .alias("updates") \
  .join(df.alias("{{ mapping.target[0].table }}"), "bk_hash") \
  .where("{{ mapping.target[0].table }}.current = true AND updates.row_hash <> {{ mapping.target[0].table }}.row_hash")

#определяем записи удаленные
#delete
delEvent = df \
  .alias("{{ mapping.target[0].table }}") \
  .join(updatesDF.alias("updates"), "bk_hash","left") \
  .where("{{ mapping.target[0].table }}.current = true and updates.bk_hash is null")

#формируем набор записей для merge
stagedUpdates = (
  newEvent
  .selectExpr("NULL as mergeKey", {% for mapping in mapping.mapping_ods|sort(attribute='serial') %}  "updates.{{ mapping.source[0].column }}",{% endfor %}"updates.bk_hash","updates.row_hash")   # Rows for new version
  .union(delEvent.selectExpr("bk_hash as mergeKey",{% for mappingr in mapping.mapping_ods|sort(attribute='serial') %} "{{mapping.target[0].table}}.{{ mappingr.target }}",{% endfor %}"{{mapping.target[0].table}}.bk_hash","{{mapping.target[0].table}}.row_hash"))  # Rows for disactive deleted
  .union(updatesDF.alias("updates").selectExpr("updates.bk_hash as mergeKey",{% for mapping in mapping.mapping_ods|sort(attribute='serial') %}  "updates.{{ mapping.source[0].column }}",{% endfor %}"updates.bk_hash","updates.row_hash"))  # Rows for disactive old version
)

#осуществляем мердже
deltaTable.alias("{{ mapping.target[0].table }}").merge(
  stagedUpdates.alias("staged_updates"),
  "{{ mapping.target[0].table }}.bk_hash = mergeKey") \
.whenMatchedUpdate(
  condition = "{{ mapping.target[0].table }}.current = true AND {{ mapping.target[0].table }}.row_hash <> staged_updates.row_hash",
  set = {
    "current": "false",
    "valid_to": current_date()
  }
).whenNotMatchedInsert(
  values = {
    {% for mapping in mapping.mapping_ods|sort(attribute='serial') %}"{{ mapping.target }}":"staged_updates.{{mapping.source[0].column}}"  ,
    {%endfor%}
    "bk_hash":"staged_updates.bk_hash",
    "row_hash": "staged_updates.row_hash",
    "current": "true",
    "valid_from": current_date(),
    "valid_to": "null"
  }
).execute()

print("############ result ###############")
deltaTable.toDF().orderBy("eventId","valid_from").show(truncate=False)