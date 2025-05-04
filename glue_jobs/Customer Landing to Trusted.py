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

# Script generated for node Customer Landing
CustomerLanding_node1746346707223 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="CustomerLanding_node1746346707223")

# Script generated for node Share for Research
SqlQuery3568 = '''
select * from myDataSource
where shareWithResearchAsOfDate is not null
'''
ShareforResearch_node1746346721309 = sparkSqlQuery(glueContext, query = SqlQuery3568, mapping = {"myDataSource":CustomerLanding_node1746346707223}, transformation_ctx = "ShareforResearch_node1746346721309")

# Script generated for node Customer Trusted
EvaluateDataQuality().process_rows(frame=ShareforResearch_node1746346721309, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1746346272559", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerTrusted_node1746347171493 = glueContext.getSink(path="s3://stedi-survesh-s3/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1746347171493")
CustomerTrusted_node1746347171493.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
CustomerTrusted_node1746347171493.setFormat("json")
CustomerTrusted_node1746347171493.writeFrame(ShareforResearch_node1746346721309)
job.commit()