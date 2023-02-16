# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.getOrCreate()
from pyspark.sql.types import StructField,IntegerType, StructType,StringType
df = spark.read.csv("/FileStore/tables/hospital_quarterly_financial_utilization_report_sum_of_four_quarters_9__1_.csv",header=True, inferSchema=True)
df.display()

# COMMAND ----------

df1=df.select("DIS_MCAR","DIS_MCAR_MC","DIS_MCAL","DIS_MCAL_MC","DIS_CNTY","DIS_CNTY_MC","DIS_THRD","DIS_THRD_MC","DIS_INDGNT","DIS_OTH","DIS_TOT","DAY_MCAR",
              "DAY_MCAR_MC","DAY_MCAL","DAY_MCAL_MC","DAY_CNTY","DAY_CNTY_MC","DAY_THRD","DAY_THRD_MC","DAY_INDGNT","DAY_OTH","DAY_TOT","DAY_LTC","DIS_LTC","LIC_BEDS","AVL_BEDS","STF_BEDS")
df1.dropna().display()

# COMMAND ----------

df2 = df1.withColumn("DIS_MCAR_MC", df1["DIS_MCAR_MC"].cast(IntegerType()))\
.withColumn("DIS_MCAR", df1["DIS_MCAR"].cast(IntegerType()))\
.withColumn("DIS_MCAL", df1["DIS_MCAL"].cast(IntegerType()))\
.withColumn("DIS_MCAL_MC", df1["DIS_MCAL_MC"].cast(IntegerType()))\
.withColumn("DIS_CNTY", df1["DIS_CNTY"].cast(IntegerType()))\
.withColumn("DIS_CNTY_MC", df1["DIS_CNTY_MC"].cast(IntegerType()))\
.withColumn("DIS_THRD", df1["DIS_THRD"].cast(IntegerType()))\
.withColumn("DIS_THRD_MC", df1["DIS_THRD_MC"].cast(IntegerType()))\
.withColumn("DIS_INDGNT", df1["DIS_INDGNT"].cast(IntegerType()))\
.withColumn("DIS_OTH", df1["DIS_OTH"].cast(IntegerType()))\
.withColumn("DIS_TOT", df1["DIS_TOT"].cast(IntegerType()))\
.withColumn("DAY_MCAR", df1["DAY_MCAR"].cast(IntegerType()))\
.withColumn("DAY_MCAR_MC", df1["DAY_MCAR_MC"].cast(IntegerType()))\
.withColumn("DAY_MCAL", df1["DAY_MCAL"].cast(IntegerType()))\
.withColumn("DAY_MCAL_MC", df1["DAY_MCAL_MC"].cast(IntegerType()))\
.withColumn("DAY_CNTY", df1["DAY_CNTY"].cast(IntegerType()))\
.withColumn("DAY_CNTY_MC", df1["DAY_CNTY_MC"].cast(IntegerType()))\
.withColumn("DAY_THRD", df1["DAY_THRD"].cast(IntegerType()))\
.withColumn("DAY_THRD_MC", df1["DAY_THRD_MC"].cast(IntegerType()))\
.withColumn("DAY_INDGNT", df1["DAY_INDGNT"].cast(IntegerType()))\
.withColumn("DAY_OTH", df1["DAY_OTH"].cast(IntegerType()))\
.withColumn("DAY_TOT", df1["DAY_TOT"].cast(IntegerType()))\
.withColumn("DAY_LTC", df1["DAY_LTC"].cast(IntegerType()))\
.withColumn("DIS_LTC", df1["DIS_LTC"].cast(IntegerType()))\
.withColumn("LIC_BEDS", df1["LIC_BEDS"].cast(IntegerType()))\
.withColumn("AVL_BEDS", df1["AVL_BEDS"].cast(IntegerType()))\
.withColumn("STF_BEDS", df1["STF_BEDS"].cast(IntegerType()))

# COMMAND ----------

df2.display()

# COMMAND ----------

df2.printSchema()

# COMMAND ----------

Average_Length_of_Stay = df2.select("DAY_TOT","DIS_TOT")\
    .withColumn("ALOS", df2.DAY_TOT/df2.DIS_TOT)
Average_Length_of_Stay.dropna().display()

# COMMAND ----------

Average_Length_of_Stay_excluding_LTC= df2.select("DAY_TOT","DAY_LTC","DIS_TOT","DIS_LTC")\
    .withColumn("ALOS(excluding_LTC)", (df2.DAY_TOT-df2.DAY_LTC)/(df2.DIS_TOT-df2.DIS_LTC))
Average_Length_of_Stay_excluding_LTC.dropna().display()

# COMMAND ----------

Licensed_Bed_Occupancy_Rate = df2.select("DAY_TOT","LIC_BEDS")\
.withColumn("Licensed_Bed_Occupancy_Rate", df2.DAY_TOT/(df2.LIC_BEDS*365))
Licensed_Bed_Occupancy_Rate.dropna().display()

# COMMAND ----------

Available_Bed_Occupancy_Rate=df2.select ("DAY_TOT","AVL_BEDS")\
.withColumn("Available_Bed_Occupancy_Rate", df2.DAY_TOT/(df2.AVL_BEDS*365))
Available_Bed_Occupancy_Rate.dropna().display()

# COMMAND ----------

Staffed_Bed_Occupancy_Rate = df2.select ("DAY_TOT","STF_BEDS")\
.withColumn("Staffed_Bed_Occupancy_Rate", df2.DAY_TOT/(df2.STF_BEDS*365))
Staffed_Bed_Occupancy_Rate.dropna().display()

# COMMAND ----------

Occupied_Beds_Average_Daily_Census= df2.select("LIC_BEDS")\
.withColumn("Occupied_Beds_Average_Daily_Census", df2.LIC_BEDS*Licensed_Bed_Occupancy_Rate)
Occupied_Beds_Average_Daily_Census.dropna().display()

# COMMAND ----------

df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/ranjitapatil09@gmail.com/hospital.csv")
df1.printSchema()

# COMMAND ----------

df1.drop("FAC_NO","FAC_NAME","YEAR_QTR","BEG_DATE","END_DATE","OP_STATUS","PHONE","ADDRESS","CITY","ZIP_CODE","CEO","TYPE_CNTRL","TYPE_HOSP","TEACH_RURL") \
    .printSchema()
df1.display()

