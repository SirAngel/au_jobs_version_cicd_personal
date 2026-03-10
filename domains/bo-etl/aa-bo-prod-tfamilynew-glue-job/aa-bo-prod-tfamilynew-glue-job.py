import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime
import json
import boto3

# Inicializar contextos
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Configuraciones
schema_name = "embol"
table_name = "family"
database_name = "embol_aa_curated"
secret_name = "embonor-AA/prod/pedidosugerido"  # Cambiar por el nombre del secret

def get_productos_data():
    """Consulta datos de productos desde Athena usando Glue Catalog"""
    # Obtener max timestamps primero
    max_ts_prod = spark.sql("SELECT MAX(process_timestamp) as max_ts FROM embol_aa_curated.d_prod").collect()[0]['max_ts']
    max_ts_marca = spark.sql("SELECT MAX(process_timestamp) as max_ts FROM embol_aa_curated.d_marca").collect()[0]['max_ts']
    max_ts_tamano = spark.sql("SELECT MAX(process_timestamp) as max_ts FROM embol_aa_curated.d_tamano").collect()[0]['max_ts']
    max_ts_empaque = spark.sql("SELECT MAX(process_timestamp) as max_ts FROM embol_aa_curated.d_tipo_empaque").collect()[0]['max_ts']
    max_raw_empaque = spark.sql("SELECT MAX(raw_timestamp) as max_ts FROM embol_aa_curated.d_tipo_empaque").collect()[0]['max_ts']
    max_ts_sabor = spark.sql("SELECT MAX(process_timestamp) as max_ts FROM embol_aa_curated.d_sabor").collect()[0]['max_ts']
    max_ts_catego = spark.sql("SELECT MAX(process_timestamp) as max_ts FROM embol_aa_curated.d_categoria").collect()[0]['max_ts']
    max_ts_consum = spark.sql("SELECT MAX(process_timestamp) as max_ts FROM embol_aa_curated.d_tipo_consum").collect()[0]['max_ts']
    
    query = f"""
        SELECT 
            PROD.id_prod,
            CATEGO.desc_catego AS categoria,
            CONCAT(MARCA.desc_marca, '-', PROD.azucar) AS marcamkt,
            CONCAT(EMP.desc_tipo_empaque_gen, '-', CONSUM.desc_tipo_consum_corto) AS formato,
            CONCAT(EMP.desc_tipo_empaque, '-', TAM.desc_tamano) AS formatomkt,
            PROD.bote AS cantsubunidades,
            PROD.sku,
            PROD.sku_padre,
            SABOR.desc_sabor,
            CONCAT(MARCA.desc_marca, ' ', SABOR.desc_sabor, ' ', TAM.desc_tamano, ' ', EMP.desc_tipo_empaque, ' ', 'BOTELLAS: ', CAST(PROD.bote AS string)) AS name_concat,
            CASE
                WHEN PROD.azucar LIKE '%SIN A%' THEN CONCAT(MARCA.desc_marca, ' ', PROD.azucar, ' ', CAST(TAM.desc_tamano AS string), ' ', EMP.desc_tipo_empaque)
                ELSE CONCAT(MARCA.desc_marca, ' ', CAST(TAM.desc_tamano AS string), ' ', EMP.desc_tipo_empaque)
            END AS name_concat_general
        FROM embol_aa_curated.d_prod AS PROD
            LEFT JOIN embol_aa_curated.d_marca AS MARCA
                ON CAST(MARCA.id_marca AS string) = PROD.id_marca 
                AND MARCA.process_timestamp = timestamp'{max_ts_marca}'
            LEFT JOIN embol_aa_curated.d_tamano AS TAM
                ON TAM.id_tamano = PROD.id_tamano 
                AND TAM.process_timestamp = timestamp'{max_ts_tamano}'
            LEFT JOIN embol_aa_curated.d_tipo_empaque AS EMP
                ON EMP.id_tipo_empaque = PROD.id_tipo_empaque  
                AND EMP.process_timestamp = timestamp'{max_ts_empaque}'
                AND EMP.raw_timestamp = timestamp'{max_raw_empaque}'
            LEFT JOIN embol_aa_curated.d_sabor AS SABOR
                ON SABOR.id_sabor = PROD.id_sabor 
                AND SABOR.process_timestamp = timestamp'{max_ts_sabor}'
            LEFT JOIN embol_aa_curated.d_categoria AS CATEGO
                ON CATEGO.id_catego = PROD.id_catego
                AND CATEGO.process_timestamp = timestamp'{max_ts_catego}'
            LEFT JOIN embol_aa_curated.d_tipo_consum CONSUM
                ON CONSUM.id_tipo_consum = PROD.id_tipo_consum
                AND CONSUM.process_timestamp = timestamp'{max_ts_consum}'
        WHERE 
            PROD.process_timestamp = timestamp'{max_ts_prod}'
            AND PROD.ffin_vreg > date '2025-01-01'
            AND PROD.suprimido <> 'S' 
            AND PROD.fech_supr IS NULL
            AND PROD.sku < 3000
    """
    return spark.sql(query)

def get_ventas_data():
    """Consulta datos de ventas desde Athena usando Glue Catalog"""
    # Obtener max timestamps primero
    max_ts_prod = spark.sql("SELECT MAX(process_timestamp) as max_ts FROM embol_aa_curated.d_prod").collect()[0]['max_ts']
    max_ts_localidad = spark.sql("SELECT MAX(process_timestamp) as max_ts FROM embol_aa_curated.d_localidad").collect()[0]['max_ts']
    
    query = f"""
        SELECT
            hv.id_localidad,        
            dl.agru_local,
            hv.id_prod,
            sum(hv.cajas_fisicas) AS cajas_fisicas,
            max(hv.fecha) AS ultima_compra,
            CASE 
                WHEN dl.agru_local = 'COCHABAMBA' THEN 1
                WHEN dl.agru_local = 'LA PAZ' THEN 2
                WHEN dl.agru_local = 'ORURO' THEN 3
                WHEN dl.agru_local = 'POTOSI' THEN 4
                WHEN dl.agru_local = 'SANTA CRUZ' THEN 5
                WHEN dl.agru_local = 'SUCRE' THEN 6
                WHEN dl.agru_local = 'TARIJA' THEN 7
            END AS idplanta
        FROM embol_aa_curated.h_ventas hv
        INNER JOIN embol_aa_curated.d_prod dp
            ON hv.id_prod = dp.id_prod
            AND dp.process_timestamp = timestamp'{max_ts_prod}'
            AND dp.ffin_vreg > date '2025-01-01'
            AND dp.suprimido <> 'S' 
            AND dp.fech_supr IS NULL
            AND dp.sku < 3000
        INNER JOIN embol_aa_curated.d_localidad dl
            ON hv.id_localidad = dl.id_localidad
            AND dl.process_timestamp = timestamp'{max_ts_localidad}'
        WHERE
            hv.fecha >= add_months(current_date(), -3)
            AND hv.fecha <= current_date()
            AND hv.id_tipo_transaccion IN (9,10,11,12,13)
        GROUP BY 
            hv.id_localidad,
            dl.agru_local,
            hv.id_prod
    """
    return spark.sql(query)

def process_family_data():
    """Procesa los datos para crear las familias"""
    try:
        # Cargar datos
        print("Cargando datos de ventas...")
        h_ventas = get_ventas_data()
        
        print("Cargando datos de productos...")
        d_prod = get_productos_data()
        
        # Join de datos
        print("Procesando datos...")
        ventas_productos = h_ventas.join(d_prod, "id_prod", "inner")
        ventas_productos = ventas_productos.filter(F.col("categoria") == "STILL")
        
        grupos = (
            ventas_productos
            .select("idplanta", "name_concat_general")
            .distinct()
            .withColumn(
                "n_grupo",
                F.dense_rank().over(Window.orderBy("idplanta", "name_concat_general"))  # empieza en 1
            )
        )
        
        # 4) pegar n_grupo a todas las filas
        ventas_productos = ventas_productos.join(
            grupos,
            on=["idplanta", "name_concat_general"],
            how="left"
        )
        
        # Crear familyid
        ventas_productos = ventas_productos.withColumn(
            "familyid",
            F.concat(
                F.col("idplanta").cast("string"),
                F.lit("000"),
                F.col("n_grupo").cast("string")
            ).cast("int")
        )
        
        # Eliminar duplicados
        ventas_productos = ventas_productos.dropDuplicates(["idplanta", "id_prod", "sku", "familyid"])
        
        # Agregar timestamps
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        next_day = datetime.now().strftime('%Y-%m-%d')
        
        ventas_productos = ventas_productos.withColumn("process_timestamp", F.lit(current_timestamp))
        ventas_productos = ventas_productos.withColumn("fecha", F.lit(next_day))
        
        # Seleccionar columnas para familias
        familias = ventas_productos.select(
            F.col("idplanta"),
            F.col("id_prod").alias("articulokey"),
            F.col("categoria").alias("categoriamkt"),
            F.col("marcamkt"),
            F.col("formato"),
            F.col("formatomkt"),
            F.col("cantsubunidades"),
            F.col("familyid"),
            F.col("sku"),
            F.col("sku_padre").alias("parentid"),
            F.col("process_timestamp"),
            F.col("agru_local").alias("planta"),
            F.col("fecha")
        ).dropDuplicates()
        
        # Convertir tipos de datos
        familias = familias.withColumn("articulokey", F.col("articulokey").cast("int")) \
                          .withColumn("cantsubunidades", F.col("cantsubunidades").cast("int")) \
                          .withColumn("idplanta", F.col("idplanta").cast("int")) \
                          .withColumn("familyid", F.col("familyid").cast("int")) \
                          .withColumn("sku", F.col("sku").cast("int")) \
                          .withColumn("parentid", F.col("parentid").cast("int"))
        
        # Crear tabla de relación de familias
        relacion_familias = familias.select(
            F.col("familyid").alias("family_id"),
            F.col("parentid")
        ).dropDuplicates().orderBy("family_id")
        
        print(f"Familias procesadas: {familias.count()}")
        print(f"Relaciones de familias: {relacion_familias.count()}")
        
        return familias, relacion_familias
        
    except Exception as e:
        print(f"Error en el procesamiento: {str(e)}")
        raise e
        
        
def get_secret(secret_name):
    """Obtiene credenciales de Secrets Manager"""
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

def write_to_postgres(df, table_name, credentials, mode="overwrite"):
    """Escribe DataFrame a PostgreSQL"""
    try:
        jdbc_url = f"jdbc:postgresql://{credentials['host']}:{credentials['port']}/{credentials['dbname']}"
        
        df.write \
          .format("jdbc") \
          .option("url", jdbc_url) \
          .option("dbtable", table_name) \
          .option("user", credentials['username']) \
          .option("password", credentials['password']) \
          .option("driver", "org.postgresql.Driver") \
          .option("truncate", "true") \
          .mode("overwrite") \
          .save()
        
        print(f"Datos escritos exitosamente en PostgreSQL: {table_name}")
    except Exception as e:
        print(f"Error escribiendo a PostgreSQL: {str(e)}")
        raise e

def write_to_s3(df, s3_path):
    """Escribe DataFrame a S3 en formato Parquet"""
    
    #df_out = df.withColumn("fecha", F.date_format(F.current_date(), "yyyy-MM-dd"))
    
    try:
        #df.coalesce(1).write \
        #  .mode("overwrite") \
        #  .option("compression", "gzip") \
        #  .parquet(s3_path)

        
        df.coalesce(1).write \
          .mode("overwrite") \
          .option("compression", "uncompressed") \
          .partitionBy("fecha") \
          .parquet(s3_path)
          
        print(f"Datos escritos exitosamente en {s3_path}")
        
    except Exception as e:
        print(f"Error escribiendo a S3: {str(e)}")
        raise e

# Proceso principal
if __name__ == "__main__":
    try:
        print("Iniciando procesamiento de familias...")
        
        # Procesar datos
        familias_df, relacion_familias_df = process_family_data()
        
        # Escribir resultados a S3
        s3_base_path = "s3://embol-aa-curated-f5c70f7a41da4999b25397e0ad8ad80c/embol_aa_curated/t_family_new/"
        #s3://sandbox-mchamorro/embol/family/
        
        
        write_to_s3(familias_df, f"{s3_base_path}")
        
        #s3_base_path = "s3://sandbox-mchamorro/embol/relacion_familias/"
        
        #write_to_s3(relacion_familias_df, f"{s3_base_path}")
        
        # Obtener credenciales de PostgreSQL
        print("Obteniendo credenciales de Secrets Manager...")
        pg_credentials = get_secret(secret_name)
        
        # Escribir a PostgreSQL
        write_to_postgres(relacion_familias_df, f"{schema_name}.{table_name}", pg_credentials)
        
        print("Procesamiento completado exitosamente")
        
    except Exception as e:
        print(f"Error en el job: {str(e)}")
        raise e
    finally:
        job.commit()
