#Spark Program (4B) for Demonstrating Access Control
#
# Counts occurrences of medical condition codes and Social Media messages per user
# Using dataset with PII fields ENCRYPTED
# Produces a text file and CSV data set
#
from pyspark.sql import SparkSession

from pyspark.sql.types import *
from pyspark.sql.functions import *
# Reference = https://spark.apache.org/docs/latest/sql-ref-datatypes.html

if __name__ == "__main__":

    spark=SparkSession.builder.master('yarn').appName("Analysis-Job-Encrypt").getOrCreate()
    #spark=SparkSession.builder.master('local[1]').appName("Analysis-Job-Encrypt").getOrCreate()

    encryptDataSchema = StructType([
        StructField("created_at", DateType()),
        StructField("text", StringType()),
        StructField("display", StringType()),
        StructField("code", LongType()),
        StructField("assertedDate", DateType()),
        StructField("abatementDate", DateType()),
        StructField("SynSocial_UID_encrypted", StringType()),
        StructField("name_encrypted", StringType()),
        StructField("screen_name_encrypted", StringType()),
        StructField("filename_encrypted", StringType()),
        StructField("Given_Name_encrypted", StringType()),
        StructField("Identifier_encrypted", StringType()),
        StructField("Synthea_UID_encrypted", StringType()),
        StructField("Family_Name_encrypted", StringType())    
    ])

    # The file created during the filter operation with all the patients and medical condition codes
    folderName = "hdfs:///DataSet1/encryptedDFData"
    # stored locally for test
    #folderName='/home/anne/spark-prototype/S3Data/DataSet1/encryptedDFData'

    analyzeDF = spark.read.format('csv').option('header', True).schema(encryptDataSchema).load(folderName)

    #analyzeDF.show(truncate=True)
    #analyzeDF.printSchema()

    # Create the following counts and save as CSV files 
    #  Number of Social Media Users - number of unique occurrances of SynSocial UID
    totalUsersDF = analyzeDF.select(countDistinct("SynSocial_UID_encrypted").alias("No. Social Media Message Sets"))
    #totalUsersDF.show()
    totalUsers = int(totalUsersDF.collect()[0][0])
    print ("total number of Social Media User message sets is:  ", totalUsers)

    #  Number of Medical Records Used - number of unique occurrences of Synthea UID
    totalMedRecordsDF = analyzeDF.select(countDistinct("Synthea_UID_encrypted").alias("No. Medical Record Sets"))
    #totalMedRecordsDF.show()
    totalMedRecords = int(totalMedRecordsDF.collect()[0][0])
    print ("total Number of Medical Record sets is:  ", totalMedRecords )

    # List and Number of Medical Condition Codes
    listCodesDF = analyzeDF.filter(analyzeDF.code.isNotNull()).select("code").distinct().alias("Distinct Codes")
    #listCodesDF.show()
    totalCodesDF = listCodesDF.select(countDistinct("code").alias("No. Medical Codes"))
    #totalCodesDF.show()
    totalCodes= int(totalCodesDF.collect()[0][0])
    print ("total Number of Medical Codes is:  ", totalCodes)

    #  Number of persons with each medical condition code - number of unique names for each medical condition code
    personCodeDF = analyzeDF.filter(analyzeDF.code.isNotNull()).groupBy("SynSocial_UID_encrypted", "code").count()
    #personCodeDF.drop("count")
    personCodeDF.show()
    personCodeCountDF = personCodeDF.groupBy("code").count()
    personCodeCountDF.show()

    #  For each medical condition code, count the number of messages sent when that code was occurring
    # (as previously populated in the join from the asserted date to abatement date)
    numCodeDF = analyzeDF.filter(analyzeDF.code.isNotNull()).groupBy("code").count()
    #numCodeDF.show()

    #Join the personCodeDF to the numCodeDF to identify the number of messages sent per condition per user
    joinExpression = personCodeCountDF["code"] == numCodeDF["code"]
    joinType = "inner"
    personCodeCountDF = personCodeCountDF.withColumnRenamed("count", "User_Count")
    numCodeDF = numCodeDF.withColumnRenamed("count", "Message_Count")
    analysisDF = personCodeCountDF.join(numCodeDF, joinExpression, joinType).drop(personCodeCountDF.code)
    analysisDF = analysisDF.withColumnRenamed("code", "Medical_Codes")
    #analysisDF.show()

    # Add the average number of messages sent per condition - expression =  Message_Count/User_Count
    analysisDF = analysisDF.withColumn("Message_Count", analysisDF.Message_Count.cast('float'))
    analysisDF =analysisDF.withColumn("User_Count", analysisDF.User_Count.cast('float'))

    analysisDF = analysisDF.withColumn("Avg_Msg_per_User", analysisDF.Message_Count/analysisDF.User_Count)
    analysisDF = analysisDF.withColumn("Avg_Msg_per_User", round ("Avg_Msg_per_User", 2))

    analysisDF.show()

    #Create and store the file with the statistics
    # overwrites existing files 
    # stored in HDFS
    analysisFolderName = "hdfs:///DataSet1/analysis-encrypt"
    # stored locally for test
    #analysisFolderName='/home/anne/spark-prototype/S3Data/DataSet1/analysis-encrypt'

    analysisDF.coalesce(1).write.format('csv').option('header', True).mode('overwrite').save(analysisFolderName)

    #Save Analysis Statistics
    analysisStatsSchema = StructType([
        StructField("Social-Media-Users", IntegerType()),
        StructField("Medical-Records", IntegerType()),
        StructField("Medical-Codes", IntegerType())
    ])
    dataAnalysisStats = [(totalUsers, totalMedRecords, totalCodes)]
    analysisStatsDF = spark.createDataFrame(data=dataAnalysisStats, schema = analysisStatsSchema )
    analysisStatsDF.show(truncate=True)
    analysisStatsDF.printSchema()
    analysisStatsDFFolderName = "hdfs:///DataSet1/analysis-encrypt-stats"
    #analysisStatsDFFolderName = '/home/anne/spark-prototype/S3Data/DataSet1/analysis-encrypt-stats'
    analysisStatsDF.coalesce(1).write.format('csv').option('header', True).mode('overwrite').save(analysisStatsDFFolderName)

    #analysisFileFolderName = "/home/anne/spark-prototype/S3Data/DataSet1/analysis-encrypt/analysis-encrypt-stats.txt"
    #with open (analysisFileFolderName, 'w') as f:
    #    f.write("The total number of Social Media User message sets analyzed is:  %i \n" %totalUsers)
    #    f.write("The total Number of Medical Record sets analyzed is:  %i \n" %totalMedRecords)
    #    f.write("The total Number of Medical Codes analyzed is: %i \n" %totalCodes)

    spark.stop()
