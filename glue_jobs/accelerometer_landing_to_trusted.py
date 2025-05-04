import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Customer_trusted
Customer_trusted_node1746347645848 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="Customer_trusted_node1746347645848")

# Script generated for node Accelerometer_landing
Accelerometer_landing_node1746347651562 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="Accelerometer_landing_node1746347651562")

# Script generated for node Customer Privacy Filter
SqlQuery3749 = '''
SELECT Accelerometer_landing.* 
FROM Accelerometer_landing
INNER JOIN 
Customer_trusted on Accelerometer_landing.user =Customer_trusted.email;
'''
CustomerPrivacyFilter_node1746352898581 = sparkSqlQuery(glueContext, query = SqlQuery3749, mapping = {"Accelerometer_landing":Accelerometer_landing_node1746347651562, "Customer_trusted":Customer_trusted_node1746347645848}, transformation_ctx = "CustomerPrivacyFilter_node1746352898581")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=CustomerPrivacyFilter_node1746352898581, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1746346272559", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1746350251525 = glueContext.getSink(path="s3://stedi-survesh-s3/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1746350251525")
AmazonS3_node1746350251525.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AmazonS3_node1746350251525.setFormat("json")
AmazonS3_node1746350251525.writeFrame(CustomerPrivacyFilter_node1746352898581)
job.commit()