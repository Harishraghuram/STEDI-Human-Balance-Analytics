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

# Script generated for node customer_curated
customer_curated_node1766500811831 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="customer_curated_node1766500811831")

# Script generated for node step_trainer_landing
step_trainer_landing_node1766500842865 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_training_landing", transformation_ctx="step_trainer_landing_node1766500842865")

# Script generated for node joins
SqlQuery1369 = '''
select s.serialNumber,
s.sensorReadingTime, s.distanceFromObject
from s
join c
on s.serialNumber = c.serialNumber

'''
joins_node1766500872748 = sparkSqlQuery(glueContext, query = SqlQuery1369, mapping = {"s":step_trainer_landing_node1766500842865, "c":customer_curated_node1766500811831}, transformation_ctx = "joins_node1766500872748")

# Script generated for node drop duplicates
SqlQuery1370 = '''
select serialNumber,sensorReadingTime,
distanceFromObject from j

'''
dropduplicates_node1766501003581 = sparkSqlQuery(glueContext, query = SqlQuery1370, mapping = {"j":joins_node1766500872748}, transformation_ctx = "dropduplicates_node1766501003581")

# Script generated for node step_trainer trusted
EvaluateDataQuality().process_rows(frame=dropduplicates_node1766501003581, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1766500776657", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
step_trainertrusted_node1766501102864 = glueContext.getSink(path="s3://stedi-project-unique/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainertrusted_node1766501102864")
step_trainertrusted_node1766501102864.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
step_trainertrusted_node1766501102864.setFormat("json")
step_trainertrusted_node1766501102864.writeFrame(dropduplicates_node1766501003581)
job.commit()