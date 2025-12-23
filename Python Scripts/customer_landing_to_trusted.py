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

# Script generated for node customer_landing
customer_landing_node1766498883029 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="customer_landing_node1766498883029")

# Script generated for node privacy filter
SqlQuery1379 = '''
select * from c
where shareWithResearchAsOfDate is NOT NULL;

'''
privacyfilter_node1766498899050 = sparkSqlQuery(glueContext, query = SqlQuery1379, mapping = {"c":customer_landing_node1766498883029}, transformation_ctx = "privacyfilter_node1766498899050")

# Script generated for node customer_trusted
EvaluateDataQuality().process_rows(frame=privacyfilter_node1766498899050, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1766498844835", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_trusted_node1766499007612 = glueContext.getSink(path="s3://stedi-project-unique/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_trusted_node1766499007612")
customer_trusted_node1766499007612.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
customer_trusted_node1766499007612.setFormat("json")
customer_trusted_node1766499007612.writeFrame(privacyfilter_node1766498899050)
job.commit()