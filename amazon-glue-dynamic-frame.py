import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit
from pyspark.sql.functions import *



## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])


#my_spark = SparkSession.builder.appName("myApp").config("spark.mongodb.input.uri", "mongodb://ec2-3-15-0-201.us-east-2.compute.amazonaws.com/student.emp").config("spark.mongodb.output.uri", "mongodb://ec2-3-15-0-201.us-east-2.compute.amazonaws.com/student.emp").getOrCreate()
#df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

         
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## @type: DataSource
## @args: [database = "test", table_name = "pre_employee1", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "test", table_name = "pre_employee1", transformation_ctx = "datasource0").toDF()

datasource0_year = datasource0.withColumn("Year", year(datasource0["dt"]))
datasource0_month = datasource0_year.withColumn("Month", month(datasource0_year["dt"]))
datasource0_day = datasource0_month.withColumn("Day", dayofyear(datasource0_month["dt"]))
df = glueContext.create_dynamic_frame.from_catalog(database = "test", table_name = "pre2employee3", transformation_ctx = "df").toDF()

datasource0_final = datasource0_day.join(df,(datasource0_day.id==df.empid),'inner')



#from_unixtime(unix_timestamp('date_str', 'MM/dd/yyy'))
datasource1=DynamicFrame.fromDF(datasource0_final, glueContext,"nested")


## @type: ApplyMapping
## @args: [mapping = [("id", "long", "id", "long"), ("name", "string", "name", "string"), ("age", "long", "age", "long"), ("dt", "string", "dt", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource1, mappings = [("id", "long", "id", "long"),("carno", "long", "carno", "long"), ("name", "string", "name", "string"), ("age", "long", "age", "long"), ("Year", "string", "Year", "string"),("Month", "string", "Month", "string"),("Day","string", "Day","string"),("dt", "string", "dt", "string")], transformation_ctx = "applymapping1")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://employee1"}, format = "csv", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = applymapping1]
datasink2 = glueContext.write_dynamic_frame.from_options(frame = applymapping1, connection_type = "s3", connection_options = {"path": "s3://employee1"}, format = "csv", transformation_ctx = "datasink2")
job.commit()
