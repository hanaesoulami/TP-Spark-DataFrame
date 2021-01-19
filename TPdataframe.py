#Start a simple Spark Session
import findspark
findspark.init("C:/spark")
import pyspark 
findspark.find()

#importer spark Session
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

#1- Start a simple Spark Session
spark = SparkSession.builder \
                    .master("local") \
                    .appName("FolksDF") \
                    .getOrCreate()

#2 Load the Walmart Stock CSV File
Walmart = spark.read \
                .option("header",'true') \
                .option("delimiter",",") \
                .csv("walmart_stock.csv")
Walmart.show()

#3-the column names
Walmart.columns

#4-the Schema look like:
Walmart.printSchema()

#5-Create a new dataframe with a column called "HV_Ratio" that is the ratio of the High Price versus volume of stock traded for a day
WalmartNew = Walmart.withColumn("HV_Ratio", F.col("High")/F.col("Volume"))
WalmartNew.head() 


#show the new dataframe
WalmartNew.show()

#6-The day  that had the Peak High in Price
#en SQL 
# transformation du dataframe en table
WalmartNew.createOrReplaceTempView("WalmartSQL") 
spark.sql("""select Date from WalmartSQL order by High desc limit 1""").show()

#en DSL 
WalmartNew.select(F.col("Date")) \
          .orderBy(F.col("High").desc()) \
          .head()          


#7-The mean of the Close column
# en SQL
spark.sql("""select mean(Close) as Moyenne from WalmartSQL""").show()

# en DSL
WalmartNew.agg(F.mean("Close") \
          .alias("Moyenne")) \
          .show()       

#8-The max and min of the Volume column
#en SQL
spark.sql("""select max(Volume) as Max, min(Volume) sas Min from WalmartSQL""").show()

#en DSL
WalmartNew.agg(F.max("Volume").alias("Max"),F.min("Volume").alias("Min")) \
          .show()


9-The days that were the Close lower than 60 dollars
#en SQL
spark.sql("""select count(Date) as Nb from WalmartSQL where Close < '60' """).show()

#en DSL
WalmartNew.filter(F.col("Close") < '60') \
          .count()          

#10-The percentage of the time that was the High greater than 80 dollars.(In other words, (Number of Days High>80)/(Total Days in the dataset))
#en SQL
spark.sql(""" select round(
                            (select count(*) from WalmartSQL
                                where High>='80') / (count(*))* 100
                        , 2)  as Pourcentage from WalmartSQL""").show()

#en DSL
Temporaire = WalmartNew.filter(F.col("High")>'80') \
                        .agg(F.count("*").alias ("Comptage")) \
                        .collect()[0][0] 
WalmartNew.agg(F.round((Temporaire/F.count("*")*100),2).alias("Pourcentage")) \
          .show()



#11-The max High per year
#en SQL
WalmartNew.agg(F.round((Temporaire/F.count("*")*100),2).alias("Pourcentage")) \
          .show()
        
#en DSL
WalmartNew = WalmartNew.withColumn("Annee",F.substring("Date",1,4))
WalmartNew.select("Date", "Annee").show(4)

WalmartNew.groupBy(F.col("Annee")) \
          .agg(F.max("High")) \
          .orderBy(F.col("Annee").asc()) \
          .show()

# 12-The average Close for each Calendar Month, In other words, across all the years, the average Close price for Jan,Feb, Mar, etc... the result will have a value for each of these months.
#en SQL
spark.sql(""" select  substr(Date,6,2) as Mois, mean(Close) as Moyenne from WalmartSQL group by Mois order by Mois asc """).show()

#en DSL
WalmartNew = WalmartNew.withColumn("Mois",F.substring("Date",6,2))

WalmartNew.groupBy(F.col("Mois")) \
          .agg(F.mean("Close").alias("Moyenne")) \
          .orderBy(F.col("Mois").asc()) \
          .show()