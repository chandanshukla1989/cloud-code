import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit
from pyspark.sql.functions import *



#job name
args = getResolvedOptions(sys.argv, ['JOB_NAME'])



sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

         
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
#initialise data frame from dynamic frame to append columns
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "test", table_name = "pre_employee1", transformation_ctx = "datasource0").toDF()

datasource0_year = datasource0.withColumn("Year", year(datasource0["dt"]))
datasource0_month = datasource0_year.withColumn("Month", month(datasource0_year["dt"]))
datasource0_day = datasource0_month.withColumn("Day", dayofyear(datasource0_month["dt"]))
 
df = glueContext.create_dynamic_frame.from_catalog(database = "test", table_name = "pre2employee3", transformation_ctx = "df").toDF()
#convert final data frame to dynamic frame
datasource0_final = datasource0_day.join(df,(datasource0_day.id==df.empid),'inner')



 datasource1=DynamicFrame.fromDF(datasource0_final, glueContext,"nested")


applymapping1 = ApplyMapping.apply(frame = datasource1, mappings = [("id", "long", "id", "long"),("carno", "long", "carno", "long"), ("name", "string", "name", "string"), ("age", "long", "age", "long"), ("Year", "string", "Year", "string"),("Month", "string", "Month", "string"),("Day","string", "Day","string"),("dt", "string", "dt", "string")], transformation_ctx = "applymapping1")

datasink2 = glueContext.write_dynamic_frame.from_options(frame = applymapping1, connection_type = "s3", connection_options = {"path": "s3://employee1"}, format = "csv", transformation_ctx = "datasink2")
job.commit()
