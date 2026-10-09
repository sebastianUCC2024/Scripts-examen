import shutil
from pathlib import Path
import pyarrow.parquet as parquet
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, upper, regexp_replace, initcap, lit,
    try_to_timestamp, when, sum, avg, count, desc,count_distinct, round as spark_round
)

spark = SparkSession.builder \
    .appName("ETL_vuelos") \
    .getOrCreate()

df = spark.read.option("header", True).option("inferSchema", True).csv("flights.csv")
df_airlines = spark.read.option("header", True).option("inferSchema", True).csv("airlines.csv")


#Total registros
print("Registros originales:", df.count())

#Total de vuelos
df.select(count("*").alias("total_vuelos")).show()

#Total de aviones con tailnum
df.select(count_distinct("tailnum").alias("total_aviones_tailnum")).show()

#Total de millas
df.filter(col("dep_time").isNotNull() & col("arr_time").isNotNull()) \
  .select(
      sum("distance").alias("total_millas"),
      (sum("air_time") / 60).alias("total_horas_vuelo")
  ).show()


#Destino mas visitado
df.groupBy("dest") \
    .agg(count("*").alias("total_vuelos")) \
    .orderBy(col("total_vuelos").desc()) \
    .show(1)

#Volumen de millas carrier
millas_por_aerolinea = (
    df.filter(col("dep_time").isNotNull() & col("arr_time").isNotNull())
    .groupBy("carrier")
    .agg(sum("distance").alias("total_millas"))
)

resultado = (
    millas_por_aerolinea.join(df_airlines, on="carrier", how="inner")
    .select("carrier","name","total_millas")
    .orderBy(col("total_millas").desc())
)

resultado.show(5, truncate=False)

#Destinos mas visitados
df.filter(col("dep_time").isNotNull() & 
          col("arr_time").isNotNull() &
          (col("origin") != "JFK")
          )\
          .groupBy("dest") \
          .agg(count("*").alias("total_vuelos")) \
          .orderBy(col("total_vuelos").desc()) \
          .show(5)



#Promedio de retraso
df.filter(col("dep_delay").isNotNull()) \
    .groupBy("carrier")