"""
Construye la OBT (One Big Table) a partir de las tablas GOLD y la guarda en Snowflake.
Grano: una fila por orden de producción (igual que fct_lotes).
"""
import os
import sys
from pyspark.sql import SparkSession, functions as F

SF = "net.snowflake.spark.snowflake"


def leer_llave(ruta):
    # El conector necesita la llave privada sin las líneas BEGIN/END
    with open(ruta) as f:
        return "".join(l.strip() for l in f if "-----" not in l)


opciones = {
    "sfURL": f"{os.environ['SNOWFLAKE_ACCOUNT']}.snowflakecomputing.com",
    "sfUser": os.environ["SNOWFLAKE_USER"],
    "sfRole": os.environ["SNOWFLAKE_ROLE"],
    "sfWarehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
    "sfDatabase": os.environ["SNOWFLAKE_DATABASE"],
    "pem_private_key": leer_llave(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]),
}

spark = SparkSession.builder.appName("obt_merma_lotes").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


def leer_gold(tabla):
    return (spark.read.format(SF).options(**opciones)
            .option("sfSchema", "GOLD").option("dbtable", tabla).load())


def prefijar(df, prefijo):
    # dim_fecha se usa dos veces: se renombran sus columnas para no repetir nombres
    return df.select([F.col(c).alias(f"{prefijo}_{c}") for c in df.columns])


# 1. Leer el esquema estrella
fct = leer_gold("FCT_LOTES")
dim_estilo = leer_gold("DIM_ESTILO")
dim_fecha = leer_gold("DIM_FECHA")
fecha_coccion = prefijar(dim_fecha, "COCCION")
fecha_cierre = prefijar(dim_fecha, "CIERRE")

# 2. Unir hechos con dimensiones (left join: nunca se pierde una orden)
obt = (
    fct.join(dim_estilo, on="ESTILO", how="left")
       .join(fecha_coccion, F.col("FECHA_COCCION") == F.col("COCCION_FECHA"), "left")
       .drop("COCCION_FECHA")
       .join(fecha_cierre, F.col("FECHA_CIERRE") == F.col("CIERRE_FECHA"), "left")
       .drop("CIERRE_FECHA")
)

# 3. Validar los joins ANTES de escribir
filas_fct = fct.count()
filas_obt = obt.count()
ordenes_unicas = obt.select("NUMERO_DOCUMENTO").distinct().count()
sin_estilo = obt.filter(F.col("CEPA").isNull()).count()
sin_fecha = obt.filter(F.col("COCCION_MES").isNull() | F.col("CIERRE_MES").isNull()).count()

print("========== VALIDACIÓN DE LA OBT ==========")
print(f"Filas en fct_lotes:           {filas_fct}")
print(f"Filas en la OBT:              {filas_obt}")
print(f"Órdenes únicas en la OBT:     {ordenes_unicas}")
print(f"Filas sin dimensión estilo:   {sin_estilo}")
print(f"Filas sin dimensión fecha:    {sin_fecha}")

errores = []
if filas_obt != filas_fct:
    errores.append("Los joins cambiaron el número de filas")
if ordenes_unicas != filas_obt:
    errores.append("Hay órdenes duplicadas")
if sin_estilo or sin_fecha:
    errores.append("Hay órdenes sin su dimensión")

if errores:
    print("VALIDACIÓN FALLIDA:", "; ".join(errores))
    spark.stop()
    sys.exit(1)
print("Validación OK: el grano se mantiene (una fila por orden)")

# 4. Guardar en Snowflake
(obt.write.format(SF).options(**opciones)
    .option("sfSchema", "OBT").option("dbtable", "OBT_MERMA_LOTES")
    .mode("overwrite").save())
print(f"OBT guardada en {opciones['sfDatabase']}.OBT.OBT_MERMA_LOTES")

spark.stop()
