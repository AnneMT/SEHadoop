#Spark Program (3) for Demonstrating Access Control
#
# Produces two data sets, one with PII fields hashed
# and one with PII fields encrypted
#
from pyspark.sql import SparkSession

from pyspark.sql.types import *
# Reference = https://spark.apache.org/docs/latest/sql-ref-datatypes.html

from pyspark.sql.functions import *
#from pyspark.sql.functions import udf
import hashlib

# Encrypt Column using User Defined Function (UDF)
# Reference:  databricks.com/notebooks/enforcing-column-level-encryption.html
# requires pip install fernet and pip install cryptography

from cryptography.fernet import Fernet
import datetime


def encrypt_value(clear_text):
    if clear_text is not None:
        clear_text_b=bytes(clear_text, 'utf-8')
        cipher_text = f.encrypt(clear_text_b)
        cipher_text = str(cipher_text.decode('ascii'))
        return cipher_text
    else: 
        return None 

if __name__ == "__main__":

    spark=SparkSession.builder.master('yarn').appName("Privacy-Program").getOrCreate()
    #spark=SparkSession.builder.master('local[1]').appName("Privacy-Program").getOrCreate()

    joinDataSchema = StructType([
        StructField("SynSocial_UID", StringType()),
        StructField("name", StringType()),
        StructField("screen_name", StringType()),
        StructField("created_at", DateType()),
        StructField("text", StringType()),
        StructField("filename", StringType()),
        StructField("display", StringType()),
        StructField("code", LongType()),
        StructField("assertedDate", DateType()),
        StructField("abatementDate", DateType()),
        StructField("Given_Name", StringType()),
        StructField("Identifier", StringType()),
        StructField("Synthea_UID", StringType()),
        StructField("Family_Name", StringType())    
    ])

    # The file created during the join operation with all the patients and medical condition codes
    # stored in HDFS
    folderName = "hdfs:///DataSet1/joinedDFData"
    # stored locally for test
    #folderName = '/home/anne/spark-prototype/S3Data/DataSet1/joinedDFData'

    hashDF = spark.read.format('csv').option('header', True).schema(joinDataSchema).load(folderName)

    encryDF = spark.read.format('csv').option('header', True).schema(joinDataSchema).load(folderName)


    #Using the pyspark.sql.functions.hash function 
    # Reference https://spark.apache.org/docs/latest/api/python//reference/api/pyspark.sql.functions.hash.html
    # Reference https://stackoverflow.com/questions/67041501/what-hash-algorithm-is-used-in-pyspark-sql-functions-hash
    hashDF = hashDF.withColumn('SynSocial_UID_hashed', hash('SynSocial_UID')).drop(hashDF['SynSocial_UID'])
    hashDF = hashDF.withColumn('name_hashed', hash('name')).drop(hashDF['name'])
    hashDF = hashDF.withColumn('screen_name_hashed', hash('screen_name')).drop(hashDF['screen_name'])
    hashDF = hashDF.withColumn('filename_hashed', hash('filename')).drop(hashDF['filename'])
    hashDF = hashDF.withColumn('Given_Name_hashed', hash('Given_Name')).drop(hashDF['Given_Name'])
    hashDF = hashDF.withColumn('Identifier_hashed', hash('Identifier')).drop(hashDF['Identifier'])
    hashDF = hashDF.withColumn('Synthea_UID_hashed', hash('Synthea_UID')).drop(hashDF['Synthea_UID'])
    hashDF = hashDF.withColumn('Family_Name_hashed', hash('Family_Name')).drop(hashDF['Family_Name'])

    #Create and store the encryption/decryption key
    MASTER_KEY = Fernet.generate_key()
    print ("Save the following Symetric Encryption/Decryption Key", MASTER_KEY)
    #keyFileFolderName = "/home/hdfs/DataSet1/KEY/key-info.key"
    #keyInfoFileFolderName = "/home/hdfs/DataSet1/KEY/key-readme.txt"
    #keyFileFolderName = "hdfs:///DataSet1/KEY/key-info.key"
    #keyInfoFileFolderName = "hdfs:///DataSet1/KEY/key-readme.txt"
    #keyFileFolderName = '/home/anne/spark-prototype/S3Data/DataSet1/KEY/key-info.key'
    #keyInfoFileFolderName = 'home/anne/spark-prototype/S3Data/DataSet1/KEY/key-readme.txt'
    #with open (keyFileFolderName, 'w+b') as f1:
    #    f1.write(MASTER_KEY)
    #with open (keyInfoFileFolderName, 'w') as f2:
    #    f2.write("The binary Symetric Encryption/Decryption Key was used on %s: \n" %datetime.datetime.now())
    keySchema = StructType([
        StructField("Key", BinaryType()),
        StructField("Key-String", StringType()),
        StructField("Date", DateType()),
        StructField("Note", StringType())
    ])
    dateKey = datetime.datetime.now()
    noteKey = "binary symetric encryption/decryption key"
    dataKey = [(MASTER_KEY, str(MASTER_KEY), dateKey, noteKey)]
    keyDF = spark.createDataFrame(data=dataKey, schema = keySchema)
    keyDF.show(truncate=True)
    keyDF.printSchema()
    keyDFFolderName = "hdfs:///DataSet1/KEY"
    #keyDFFolderName = '/home/anne/spark-prototype/S3Data/DataSet1/KEY'
    keyDF.coalesce(1).write.format('csv').option('header', True).mode('overwrite').save(keyDFFolderName)


    f=Fernet(MASTER_KEY)

    encrypt_udf = udf(encrypt_value, StringType()) ## UDF declaration
    encryDF = encryDF.withColumn('SynSocial_UID_encrypted', encrypt_udf('SynSocial_UID')).drop(encryDF['SynSocial_UID'])
    encryDF = encryDF.withColumn('name_encrypted', encrypt_udf('name')).drop(encryDF['name'])
    encryDF = encryDF.withColumn('screen_name_encrypted', encrypt_udf('screen_name')).drop(encryDF['screen_name'])
    encryDF = encryDF.withColumn('filename_encrypted', encrypt_udf('filename')).drop(encryDF['filename'])
    encryDF = encryDF.withColumn('Given_Name_encrypted', encrypt_udf('Given_Name')).drop(encryDF['Given_Name'])
    encryDF = encryDF.withColumn('Identifier_encrypted', encrypt_udf('Identifier')).drop(encryDF['Identifier'])
    encryDF = encryDF.withColumn('Synthea_UID_encrypted', encrypt_udf('Synthea_UID')).drop(encryDF['Synthea_UID'])
    encryDF = encryDF.withColumn('Family_Name_encrypted', encrypt_udf('Family_Name')).drop(encryDF['Family_Name'])
    
    hashDF.show(truncate=True)
    hashDF.printSchema()

    encryDF.show(truncate=False)
    encryDF.printSchema()

    # Write to HDFS (or local for testing) to Save the created hashed and encrypted data sets
    # overwrites existing files 
    # stored in HDFS
    hashedDFFolderName = "hdfs:///DataSet1/hashedDFData"
    encryptedDFFolderName = "hdfs:///DataSet1/encryptedDFData"
    # stored locally for test
    #hashedDFFolderName='/home/anne/spark-prototype/S3Data/DataSet1/hashedDFData'
    #encryptedDFFolderName='/home/anne/spark-prototype/S3Data/DataSet1/encryptedDFData'

    hashDF.write.format('csv').option('header', True).mode('overwrite').save(hashedDFFolderName)
    encryDF.write.format('csv').option('header', True).mode('overwrite').save(encryptedDFFolderName)
    spark.stop()
