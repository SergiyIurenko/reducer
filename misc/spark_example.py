import os

pp = os.getenv("PYTHONPATH")
sh = os.getenv("SPARK_HOME")
pp = sh+os.sep+"python;"+sh+os.sep+"python"+os.sep+"lib"+os.sep+"py4j-0.10.9.3-src.zip;"+pp
os.environ["PYTHONPATH"] = pp

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, input_file_name, split, slice, concat, lit

spark = SparkSession.builder.appName("reducer").getOrCreate()

df = spark.read.format("csv") \
     .option("header", True) \
     .load(r"c:\users\sergii.iurenko\PycharmProjects\Reducer\data\*.csv").\
     withColumn("filename", input_file_name())
sp = slice(split(split(slice(split(df["filename"], "[/]"), -1, 1)[0], "\\.csv")[0], "%20"), 1, 2)
sp = concat(sp[0], lit(" "), sp[1])
df = df.withColumn("Environment", sp)
ip = split(df["Source IP"], "\\.")
df = df.withColumn("IP3", ip[0].cast("int")).\
     withColumn("IP2", ip[1].cast("int")).\
     withColumn("IP1", ip[2].cast("int")).\
     withColumn("IP0", ip[3].cast("int")).\
     select("Source IP", "Environment", "IP3", "IP2", "IP1", "IP0").\
     sort("IP3", "IP2", "IP1", "IP0", "Environment").\
     select("Source IP", "Environment").\
     repartition(1).\
     write.format("csv").\
     option("header", True).\
     save(r"c:\users\sergii.iurenko\PycharmProjects\Reducer\data\result.csv")
