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

# Script generated for node customer_trusted
customer_trusted_node1766500215363 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customer_trusted_node1766500215363")

# Script generated for node accelerometer_landing
accelerometer_landing_node1766500164646 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="accelerometer_landing_node1766500164646")

# Script generated for node Joins
SqlQuery1649 = '''
select * from a join c
on a.user = c.email

'''
Joins_node1766500240363 = sparkSqlQuery(glueContext, query = SqlQuery1649, mapping = {"a":accelerometer_landing_node1766500164646, "c":customer_trusted_node1766500215363}, transformation_ctx = "Joins_node1766500240363")

# Script generated for node drop duplicates
SqlQuery1650 = '''
select distinct customerName,
email, phone, birthDay,
serialNumber, registrationDate,
lastUpdateDate, shareWithResearchAsOfDate,
shareWithPublicAsOfDate,
shareWithFriendsAsOfDate from j

'''
dropduplicates_node1766500293097 = sparkSqlQuery(glueContext, query = SqlQuery1650, mapping = {"j":Joins_node1766500240363}, transformation_ctx = "dropduplicates_node1766500293097")

# Script generated for node customer_curated
EvaluateDataQuality().process_rows(frame=dropduplicates_node1766500293097, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1766500132646", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_curated_node1766500490198 = glueContext.getSink(path="s3://stedi-project-unique/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_curated_node1766500490198")
customer_curated_node1766500490198.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
customer_curated_node1766500490198.setFormat("json")
customer_curated_node1766500490198.writeFrame(dropduplicates_node1766500293097)
job.commit()