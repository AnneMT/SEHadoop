#Spark Program (1A) for Demonstrating Access Control
#
#Reads Synthea generated HL7-FHIR medical record files and
#Outputs a CSV file with Patient Name, Condition Codes and Dates 
#
#
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

#Add to Spark Submit for pyspark --jars /opt/bunsen/jars/bunsen-shaded-0.5.9.jar
from bunsen.stu3.bundles import load_from_directory, extract_entry


if __name__ == "__main__":

    # SparkSession for operation on YARN - Hadoop Cluster or locally (for test)
    spark = SparkSession.builder.master('yarn').appName("Filter-Job-Synthea").getOrCreate()

    #spark = SparkSession.builder.appName("Filter-Job-Synthea").master("local[1]").getOrCreate()

    # The location of the Synthea files with patient names and medical condition codes
    # stored in HDFS in cluster execution
    folder = "hdfs:///DataSet1/patients"
    # stored locally (for test)
    #folder='/home/anne/spark-prototype/S3Data/DataSet1/patients'

    bundles = load_from_directory(spark, folder)
    conditions = extract_entry(spark, bundles, 'Condition')
    patients = extract_entry(spark, bundles,'Patient')

    DF1 = conditions.select('code.coding.code', 'code.coding.display', 'assertedDate', 'abatement.dateTime', 'subject.reference')
    DF1 = DF1.select(explode(DF1.code), DF1.display, DF1.assertedDate, DF1.dateTime, DF1.reference) 
    DF1 = DF1.withColumnRenamed("col", "code")
    DF1 = DF1.select(explode(DF1.display), DF1.code, DF1.assertedDate, DF1.dateTime, DF1.reference) 
    DF1 = DF1.withColumnRenamed("col", "display")
    DF1 = DF1.withColumnRenamed("dateTime", "abatementDate")


    DF2 = patients.select('identifier.value', 'id', 'name.family', 'name.given')
    DF2 = DF2.select(posexplode(DF2.value), DF2.id, DF2.family, DF2.given)
    DF2 = DF2.filter(DF2.pos == "0").drop("pos")
    DF2 = DF2.withColumnRenamed("col", "Identifier")
    DF2 = DF2.select(posexplode(DF2.family), DF2.Identifier, DF2.id, DF2.given)
    DF2 = DF2.filter(DF2.pos == "0").drop("pos")
    DF2 = DF2.withColumnRenamed("col", "Family_Name")
    DF2 = DF2.select(posexplode(DF2.given), DF2.Identifier, DF2.id, DF2.Family_Name)
    DF2 = DF2.filter(DF2.pos == "0").drop("pos")
    DF2 = DF2.select(posexplode(DF2.col), DF2.Identifier, DF2.id, DF2.Family_Name)
    DF2 = DF2.filter(DF2.pos == "0").drop("pos")
    DF2 = DF2.withColumnRenamed("col", "Given_Name")


    DF1.show(truncate=False)
    DF1.printSchema()
    DF2.show(truncate=False)
    DF2.printSchema()

    joinExpression = DF1["reference"] == DF2["id"]
    joinType = "inner"
    DF3 = DF1.join(DF2, joinExpression, joinType).drop(DF1["reference"]).withColumnRenamed("id", "Synthea_UID")

    DF3.show(truncate=False)
    DF3.printSchema()

    # Write to HDFS (or local for testing) to Save the created hashed and encrypted data sets
    # overwrites existing files 
    # stored in HDFS
    syntheaDFFolderName = "hdfs:///DataSet1/patientsDFData"
    # stored locally for test
    #syntheaDFFolderName='/home/anne/spark-prototype/S3Data/DataSet1/patientsDFData'

    DF3.write.format('csv').option('header', True).mode('overwrite').save(syntheaDFFolderName)

    spark.stop()
