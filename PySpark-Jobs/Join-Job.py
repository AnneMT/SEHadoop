#Spark Program (2) for Demonstrating Access Control
#
# Joins created DataFrames from stored Medical and Social Media CSV files, 
# based upon matching Name and Social Media Message Date
#

from pyspark.sql import SparkSession

from pyspark.sql.types import *
from pyspark.sql.functions import *
# Reference = https://spark.apache.org/docs/latest/sql-ref-datatypes.html

from datetime import datetime
import pytz
#from pyspark.sql.functions import udf, to_date, to_utc_timestamp

## User Defined Functions (UDF) Converting date string format
# Reference:  https://stackoverflow.com/questions/55970882/how-to-parse-twitter-date-time-string-in-pyspark
def getTwitDate(x):
    # conversion of Twitter created_at date to datetime format
    # such as Sat Dec 14 07:24:08 +0000 2009 to 2009-12-14
    if x is not None:
        return str(datetime.strptime(x,'%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=pytz.UTC).strftime("%Y-%m-%d"))
    else:
        return None

def getHL7Date(x):
    # conversion of HL7 FHIR dates to datime format
    # such as 
    if x is not None:
        y=x.split("T")[0]
        return str(datetime.strptime(y,'%Y-%m-%d').replace(tzinfo=pytz.UTC).strftime("%Y-%m-%d"))
    else:
        return None


if __name__ == "__main__":

    spark=SparkSession.builder.master('yarn').appName("Join-Program").getOrCreate()
    #spark=SparkSession.builder.appName("Join-Program").master('local[1]').getOrCreate()

    medicalSchema = StructType([
        StructField("display", StringType()),
        StructField("code", LongType()),
        StructField("assertedDate", StringType()),
        StructField("abatementDate", StringType()),
        StructField("Given_Name", StringType()),
        StructField("Identifier", StringType()),
        StructField("Synthea_UID", StringType()),
        StructField("Family_Name", StringType())
    ])

    # Social Media Message (Twitter) fields captured:
    # created_at, text, user.id, user.name, user.screen_name
    socialMediaSchema = StructType([
        StructField("SynSocial_UID", StringType()),
        StructField("name", StringType()),
        StructField("screen_name", StringType()),
        StructField("created_at", StringType()),
        StructField("text", StringType()),
        StructField("filename", StringType())
    ])


    # The file created during the filter operation with all the patients and medical condition codes
    # stored in HDFS or locally for test
    folderName1 = "hdfs:///DataSet1/patientsDFData"
    #folderName1 = '/home/anne/spark-prototype/S3Data/DataSet1/patientsDFData'

    # The file created during the filter operation with all the social media users and messages
    # stored in HDFS or locally for test
    folderName2 = "hdfs:///DataSet1/social-media-usersDFData"
    #folderName2 = "/home/anne/spark-prototype/S3Data/DataSet1/social-media-usersDFData/"

    medDF = spark.read.format('csv').option('header', True).schema(medicalSchema).load(folderName1)

    medDF = medDF.select(concat_ws("_", medDF.Given_Name, medDF.Family_Name, medDF.Identifier).alias("filename"),\
        "display", "code", "assertedDate", "abatementDate", "Given_Name", "Identifier", "Synthea_UID", "Family_Name")

    medDF = medDF.select(concat("filename", lit("TwitterData.json")).alias("filename"),\
        "display", "code", "assertedDate", "abatementDate", "Given_Name", "Identifier", "Synthea_UID", "Family_Name")

    socDF = spark.read.format('csv').option('header', True).schema(socialMediaSchema).load(folderName2) 
    
    date_fn1 = udf(getTwitDate, StringType())  ## UDF declaration
    socDF = socDF.withColumn("created_at", to_utc_timestamp(date_fn1("created_at"),"UTC")) 
    
    date_fn2 = udf(getHL7Date, StringType())  ## UDF declaration
    medDF = medDF.withColumn("assertedDate", to_utc_timestamp(date_fn2("assertedDate"),"UTC"))
    medDF = medDF.withColumn("abatementDate", to_utc_timestamp(date_fn2("abatementDate"),"UTC"))

    medDF.show()
    medDF.printSchema()

    socDF.show(truncate=False)
    socDF.printSchema()

    # Add the medical condition codes occurring on the dates to the social media messages of the same date
    # References: https://stackoverflow.com/questions/63311273/pyspark-joining-2-dataframes-by-id-closest-date-backwards
    
    # join lines with the same filename, when onset Date <= tweet date <= abatement Date
    joinExpression = [(medDF.filename == socDF.filename),(medDF.assertedDate <= socDF.created_at),(socDF.created_at <= medDF.abatementDate)]

    #include all the rows from the socDF, adding medDF columns with values in the rows when the joinExpression is met
    joinType = 'left_outer' 

    joinedDF = socDF.join(medDF, joinExpression, joinType).drop(medDF['filename'])

    joinedDF.show()
    joinedDF.printSchema()

    # Write to HDFS (or local for testing) to Save the created joined data set
    # overwrites existing files

    joinedDFFolderName="hdfs:///DataSet1/joinedDFData"
    #joinedDFFolderName='/home/anne/spark-prototype/S3Data/DataSet1/joinedDFData'

    joinedDF.write.format('csv').option('header', True).mode('overwrite').save(joinedDFFolderName)

    spark.stop()
