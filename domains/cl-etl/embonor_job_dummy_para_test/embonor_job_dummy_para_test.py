import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as PYSKF
from datetime import datetime, timezone, timedelta

print("Hola, esto es un test de CI/CD")
print("Hola, esto es un test de CI/CD")
print("Hola, esto es un test de CI/CD")


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

## @type: DataSource
## @args: [database = "embonor_dwh_cw_tables", table_name = "basis_olap_dbo_dim_articulo", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database="embonor_dwh_cw_tables",
    table_name="basis_olap_dbo_dim_articulo",
    transformation_ctx="datasource0",
)
## @type: ApplyMapping
## @args: [mapping = [("retornabilidadmkt", "string", "retornabilidadmkt", "string"), ("marcamkt", "string", "marcamkt", "string"), ("nombrearticulo", "string", "nombrearticulo", "string"), ("familia", "string", "familia", "string"), ("tipo_articulo", "string", "tipo_articulo", "string"), ("consumo", "string", "consumo", "string"), ("retornabilidad", "string", "retornabilidad", "string"), ("isscom", "string", "isscom", "string"), ("categoriamkt", "string", "categoriamkt", "string"), ("endulzante", "string", "endulzante", "string"), ("formatomkt", "string", "formatomkt", "string"), ("articulokey", "int", "articulokey", "int"), ("dun", "string", "dun", "string"), ("tamaño", "string", "tamaño", "string"), ("cuentacontable", "string", "cuentacontable", "string"), ("subcategoriamkt", "string", "subcategoriamkt", "string"), ("sabor", "string", "sabor", "string"), ("material", "string", "material", "string"), ("empaquemkt", "string", "empaquemkt", "string"), ("formato", "string", "formato", "string"), ("marcatm", "string", "marcatm", "string"), ("ean", "string", "ean", "string"), ("marca", "string", "marca", "string"), ("familiamkt", "string", "familiamkt", "string"), ("diakey", "int", "diakey", "int"), ("articuloid", "string", "articuloid", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(
    frame=datasource0,
    mappings=[
        ("retornabilidadmkt", "string", "retornabilidadmkt", "string"),
        ("marcamkt", "string", "marcamkt", "string"),
        ("nombrearticulo", "string", "nombrearticulo", "string"),
        ("familia", "string", "familia", "string"),
        ("tipo_articulo", "string", "tipo_articulo", "string"),
        ("consumo", "string", "consumo", "string"),
        ("retornabilidad", "string", "retornabilidad", "string"),
        ("isscom", "string", "isscom", "string"),
        ("categoriamkt", "string", "categoriamkt", "string"),
        ("endulzante", "string", "endulzante", "string"),
        ("formatomkt", "string", "formatomkt", "string"),
        ("articulokey", "int", "articulokey", "int"),
        ("dun", "string", "dun", "string"),
        ("tamaño", "string", "tamaño", "string"),
        ("cuentacontable", "string", "cuentacontable", "string"),
        ("tipo_envase", "string", "tipo_envase", "string"),
        ("subcategoriamkt", "string", "subcategoriamkt", "string"),
        ("sabor", "string", "sabor", "string"),
        ("material", "string", "material", "string"),
        ("empaquemkt", "string", "empaquemkt", "string"),
        ("formato", "string", "formato", "string"),
        ("marcatm", "string", "marcatm", "string"),
        ("ean", "string", "ean", "string"),
        ("marca", "string", "marca", "string"),
        ("familiamkt", "string", "familiamkt", "string"),
        ("diakey", "int", "diakey", "int"),
        ("articuloid", "string", "articuloid", "string"),
    ],
    transformation_ctx="applymapping1",
)

# timestamp and date partition
df_out = applymapping1.toDF()
offset = timezone(timedelta(hours=-4))
df_out = df_out.withColumn("raw_timestamp", PYSKF.lit(datetime.now(offset)))
df_out = df_out.withColumn("date_raw_partition", PYSKF.to_date(df_out.raw_timestamp))

# Write output
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df_out.write.mode("overwrite").partitionBy("date_raw_partition").json(
    path="s3://sandbox-aubilla/terraform_test/",
    compression="gzip"
)

job.commit()
