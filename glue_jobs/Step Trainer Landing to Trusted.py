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

# Script generated for node Step_trainer_landing
Step_trainer_landing_node1746361028561 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="Step_trainer_landing_node1746361028561")

# Script generated for node Customers_curated
Customers_curated_node1746361029172 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customers_curated", transformation_ctx="Customers_curated_node1746361029172")

# Script generated for node Step Joins Customer
SqlQuery3440 = '''
SELECT DISTINCT step_trainer_landing.* 
FROM step_trainer_landing
INNER JOIN customerS_curated on 
customers_curated.serialnumber=step_trainer_landing.serialnumber;
'''
StepJoinsCustomer_node1746361136229 = sparkSqlQuery(glueContext, query = SqlQuery3440, mapping = {"Step_trainer_landing":Step_trainer_landing_node1746361028561, "Customers_curated":Customers_curated_node1746361029172}, transformation_ctx = "StepJoinsCustomer_node1746361136229")

# Script generated for node step_trainer_trusted 
EvaluateDataQuality().process_rows(frame=StepJoinsCustomer_node1746361136229, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1746360994951", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
step_trainer_trusted_node1746361183317 = glueContext.getSink(path="s3://stedi-survesh-s3/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1746361183317")
step_trainer_trusted_node1746361183317.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted ")
step_trainer_trusted_node1746361183317.setFormat("json")
step_trainer_trusted_node1746361183317.writeFrame(StepJoinsCustomer_node1746361136229)
job.commit()