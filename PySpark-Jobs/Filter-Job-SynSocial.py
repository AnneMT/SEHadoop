#Spark Program (1B) for Demonstrating Access Control
#
#Reads SynSocial generated social media (Twitter) message files and
#Outputs a CSV file with User Name, Message Text and Message Date 
#
#
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F 


if __name__ == "__main__":

    # SparkSession for operation on YARN - Hadoop Cluster or locally (for test)
    spark = SparkSession.builder.master('yarn').appName("Filter-Job-SynSocial").getOrCreate()

    #spark=SparkSession.builder.appName("Filter-Job-SynSocial").master("local[1]").getOrCreate()

    # The file created during the filter operation with all the social media messages
    # stored in HDFS
    folder = "hdfs:///DataSet1/social-media-users"
    # stored locally (for test):
    #folder = "/home/anne/spark-prototype/S3Data/DataSet1/social-media-users"


    DF = spark.read.format('json').option("multiline", "true").option("mode", "PERMISSIVE").load(folder).withColumn("filename", input_file_name())
    DF = DF.select(explode(DF.messages).alias('messages'), DF.filename).select('messages.*', 'filename')
    DF = DF.select("user.id", "user.name", "user.screen_name", "created_at", "text", "filename")
    DF = DF.withColumn("filename", F.trim(F.split(DF.filename,"/")[5])) 
        # adjust for file structure, e.g., filename after 9th slash (set value to 9) for the folder for data stored locally (for test)
        # and set to 5 for hdfs folder listed above
    ##
    DF = DF.withColumnRenamed("id", "SynSocial_UID")

    DF.show()
    DF.printSchema()
    
    # Write to HDFS (or local for testing) to Save the created hashed and encrypted data sets
    # overwrites existing files 
    # stored in HDFS
    synSocialDFFolderName = "hdfs:///DataSet1/social-media-usersDFData"
    # stored locally for test
    #synSocialDFFolderName='/home/anne/spark-prototype/S3Data/DataSet1/social-media-usersDFData'

    DF.write.format('csv').option('header', True).mode('overwrite').save(synSocialDFFolderName)

    spark.stop()
