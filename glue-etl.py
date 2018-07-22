import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import lit
from awsglue.dynamicframe import DynamicFrame


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "nycitytaxi-devday18", table_name = "yellow", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "nycitytaxi-devday18", table_name = "yellow", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("vendorid", "long", "vendorid", "long"), ("tpep_pickup_datetime", "string", "pickup_date", "timestamp"), ("tpep_dropoff_datetime", "string", "dropoff_date", "timestamp"), ("passenger_count", "long", "passenger_count", "long"), ("trip_distance", "double", "trip_distance", "double"), ("ratecodeid", "long", "ratecodeid", "long"), ("store_and_fwd_flag", "string", "store_and_fwd_flag", "string"), ("pulocationid", "long", "pulocationid", "long"), ("dolocationid", "long", "dolocationid", "long"), ("payment_type", "long", "payment_type", "long"), ("fare_amount", "double", "fare_amount", "double"), ("extra", "double", "extra", "double"), ("mta_tax", "double", "mta_tax", "double"), ("tip_amount", "double", "tip_amount", "double"), ("tolls_amount", "double", "tolls_amount", "double"), ("improvement_surcharge", "double", "improvement_surcharge", "double"), ("total_amount", "double", "total_amount", "double")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("vendorid", "long", "vendorid", "long"), ("tpep_pickup_datetime", "string", "pickup_date", "timestamp"), ("tpep_dropoff_datetime", "string", "dropoff_date", "timestamp"), ("passenger_count", "long", "passenger_count", "long"), ("trip_distance", "double", "trip_distance", "double"), ("ratecodeid", "long", "ratecodeid", "long"), ("store_and_fwd_flag", "string", "store_and_fwd_flag", "string"), ("pulocationid", "long", "pulocationid", "long"), ("dolocationid", "long", "dolocationid", "long"), ("payment_type", "long", "payment_type", "long"), ("fare_amount", "double", "fare_amount", "double"), ("extra", "double", "extra", "double"), ("mta_tax", "double", "mta_tax", "double"), ("tip_amount", "double", "tip_amount", "double"), ("tolls_amount", "double", "tolls_amount", "double"), ("improvement_surcharge", "double", "improvement_surcharge", "double"), ("total_amount", "double", "total_amount", "double")], transformation_ctx = "applymapping1")
## @type: ResolveChoice
## @args: [choice = "make_struct", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2
## @inputs: [frame = applymapping1]
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_struct", transformation_ctx = "resolvechoice2")
## @type: DropNullFields
## @args: [transformation_ctx = "dropnullfields3"]
## @return: dropnullfields3
## @inputs: [frame = resolvechoice2]
dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")

##----------------------------------
#convert to a Spark DataFrame...
yellowDF = dropnullfields3.toDF()

#add a new column for "type"
yellowDF = yellowDF.withColumn("type", lit('yellow'))

# Convert back to a DynamicFrame for further processing.
yellowDynamicFrame = DynamicFrame.fromDF(yellowDF, glueContext, "yellowDF_df")
##----------------------------------

## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://<YOUR BUCKET/PATH FROM STEP1>"}, format = "parquet", transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_options(frame = yellowDynamicFrame, connection_type = "s3", connection_options = {"path": "s3://<YOUR BUCKET/PATH FROM STEP1>"}, format = "parquet", transformation_ctx = "datasink4")


####### Second DataSource

datasource22 = glueContext.create_dynamic_frame.from_catalog(database = "nycitytaxi-devday18", table_name = "csv_green", transformation_ctx = "datasource22")

applymapping22 = ApplyMapping.apply(frame = datasource22, mappings = [("vendorid", "long", "vendorid", "long"), ("lpep_pickup_datetime", "string", "pickup_date", "timestamp"), ("lpep_dropoff_datetime", "string", "dropoff_date", "timestamp"), ("store_and_fwd_flag", "string", "store_and_fwd_flag", "string"), ("ratecodeid", "long", "ratecodeid", "long"), ("pulocationid", "long", "pulocationid", "long"), ("dolocationid", "long", "dolocationid", "long"), ("passenger_count", "long", "passenger_count", "long"), ("trip_distance", "double", "trip_distance", "double"), ("fare_amount", "double", "fare_amount", "double"), ("extra", "double", "extra", "double"), ("mta_tax", "double", "mta_tax", "double"), ("tip_amount", "double", "tip_amount", "double"), ("tolls_amount", "double", "tolls_amount", "double"), ("improvement_surcharge", "double", "improvement_surcharge", "double"), ("total_amount", "double", "total_amount", "double"), ("payment_type", "long", "payment_type", "long")], transformation_ctx = "applymapping2")

resolvechoice22 = ResolveChoice.apply(frame = applymapping22, choice = "make_struct", transformation_ctx = "resolvechoice22")

dropnullfields22 = DropNullFields.apply(frame = resolvechoice22, transformation_ctx = "dropnullfields22")

##----------------------------------
#convert to a Spark DataFrame...
greenDF = dropnullfields22.toDF()

#add a new column for "type"
greenDF = greenDF.withColumn("type", lit('green'))

# Convert back to a DynamicFrame for further processing.
greenDynamicFrame = DynamicFrame.fromDF(greenDF, glueContext, "greenDF_df")
##----------------------------------

datasink22 = glueContext.write_dynamic_frame.from_options(frame = greenDynamicFrame, connection_type = "s3", connection_options = {"path": "<"}, format = "parquet", transformation_ctx = "datasink2")

####### End of Second DataSource

job.commit()
