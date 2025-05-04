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
Customer_trusted_node1746352582502 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="Customer_trusted_node1746352582502")

# Script generated for node Accelerometer_Trusted
Accelerometer_Trusted_node1746352611467 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="Accelerometer_Trusted_node1746352611467")

# Script generated for node Customer Privacy Filter
SqlQuery3433 = '''
SELECT DISTINCT Customer_trusted.* 
FROM Customer_trusted
INNER JOIN 
Accelerometer_Trusted on Customer_trusted.email =
Accelerometer_Trusted.user;
'''
CustomerPrivacyFilter_node1746353176958 = sparkSqlQuery(glueContext, query = SqlQuery3433, mapping = {"Customer_trusted":Customer_trusted_node1746352582502, "Accelerometer_Trusted":Accelerometer_Trusted_node1746352611467}, transformation_ctx = "CustomerPrivacyFilter_node1746353176958")

# Script generated for node customers_curated
EvaluateDataQuality().process_rows(frame=CustomerPrivacyFilter_node1746353176958, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1746352567983", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customers_curated_node1746353394385 = glueContext.getSink(path="s3://stedi-survesh-s3/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customers_curated_node1746353394385")
customers_curated_node1746353394385.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customers_curated")
customers_curated_node1746353394385.setFormat("json")
customers_curated_node1746353394385.writeFrame(CustomerPrivacyFilter_node1746353176958)
job.commit()