# PSet 2 · Pipeline de datos end-to-end: merma de cerveza por levadura

Pipeline ELT reproducible que lleva los datos de producción de una cervecería hasta Snowflake, listos para entrenar un modelo que prediga la **merma de filtración y levadura** de cada lote.

```
GitHub (CSV) → Kestra → RAW → dbt → BRONZE → dbt → SILVER → dbt → GOLD → Spark → OBT
```

| Capa | Esquema Snowflake | Herramienta | Contenido |
|---|---|---|---|
| Raw | `RAW` | Kestra | `MERMA_LEVADURA`: datos tal como llegan (todo texto) + metadatos de carga |
| Bronze | `BRONZE` | dbt (`models/staging`) | `STG_MERMA_LEVADURA`: tipos de datos correctos, sin quitar filas |
| Silver | `SILVER` | dbt (`models/intermedia`) | `INT_MERMA_LIMPIA`: limpieza, banderas de calidad y merma calculada |
| Gold | `GOLD` | dbt (`models/marts`) | Esquema estrella: `FCT_LOTES`, `DIM_ESTILO`, `DIM_FECHA` |
| OBT | `OBT` | Spark | `OBT_MERMA_LOTES`: una fila por orden con todas sus dimensiones |

---

## Estructura del repositorio

```
pset_2/
├── docker-compose.yml        # Postgres + Kestra + dbt + Spark
├── .env.example              # variables necesarias (sin valores secretos)
├── data/
│   └── fuente_merma_levadura_2026.csv   # fuente del pipeline
├── kestra/
│   ├── test_conexion_snowflake.yml      # prueba de conexión
│   └── ingesta_merma_raw.yml            # ingesta quincenal a RAW
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml          # conexión a Snowflake (lee todo del .env)
│   ├── macros/generate_schema_name.sql
│   ├── models/
│   │   ├── staging/          # BRONZE
│   │   ├── intermedia/       # SILVER
│   │   └── marts/            # GOLD (esquema estrella)
│   └── tests/                # tests de reglas de negocio
├── spark/
│   └── build_obt.py          # construye y valida la OBT
└── docs/
    ├── snowflake_setup.sql   # crea warehouse, base, esquemas, rol y usuario
    └── perfil_calidad.sql    # métricas de calidad de datos
```

---

## Requisitos

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (asignar al menos 6 GB de memoria)
- Git y OpenSSL
- Una cuenta de Snowflake con rol `ACCOUNTADMIN`

---

## 1. Configuración inicial (una sola vez)

### 1.1 Clonar el repositorio

```bash
git clone https://github.com/0997801643/PSET2.git
cd PSET2
```

### 1.2 Generar el par de llaves

Snowflake no permite que un usuario de servicio se conecte con contraseña, así que el pipeline usa autenticación por par de llaves. La carpeta `keys/` está en `.gitignore` y nunca se sube al repositorio.

```bash
mkdir keys
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out keys/rsa_key.p8 -nocrypt
openssl rsa -in keys/rsa_key.p8 -pubout -out keys/rsa_key.pub
```

### 1.3 Preparar Snowflake

1. Abrir `docs/snowflake_setup.sql` en una hoja SQL de Snowsight con el rol `ACCOUNTADMIN`.
2. Reemplazar `PEGA_AQUI_TU_LLAVE_PUBLICA` por el contenido de `keys/rsa_key.pub`, sin las líneas `BEGIN`/`END` y en una sola línea.
3. Reemplazar el nombre de usuario personal en el último `GRANT`.
4. Ejecutar todo el script (**Run All**).

Esto crea el warehouse `PSET2_WH`, la base `CERVECERIA_DB`, los esquemas `RAW`, `BRONZE`, `SILVER`, `GOLD` y `OBT`, el rol `PSET2_ROLE` y el usuario de servicio `PSET2_USER`.

### 1.4 Crear el archivo `.env`

```bash
cp .env.example .env
```

Completar `SNOWFLAKE_ACCOUNT` con el identificador de la cuenta (formato `ORGNAME-ACCOUNTNAME`; en Snowsight: menú de usuario → *Account* → *View account details*). Luego agregar la llave privada como secreto de Kestra (codificada en base64):

```bash
echo "SECRET_SNOWFLAKE_PRIVATE_KEY=$(base64 -i keys/rsa_key.p8 | tr -d '\n')" >> .env
```

> En Linux, usar `base64 -w0 keys/rsa_key.p8` en lugar de `base64 -i keys/rsa_key.p8 | tr -d '\n'`.

---

## 2. Levantar la infraestructura

```bash
docker compose up -d
```

Esto levanta Postgres y Kestra. dbt y Spark no quedan encendidos: se ejecutan como trabajos puntuales (ver pasos 4 y 5).

Abrir Kestra en <http://localhost:8080>:

- Usuario: `admin@kestra.io`
- Contraseña: `Admin1234`

---

## 3. Ingesta con Kestra (Fuente → RAW)

### 3.1 Cargar los flujos

En Kestra: **Flows → Create**, pegar el contenido de cada archivo de `kestra/` y guardar:

1. `test_conexion_snowflake.yml`: ejecutarlo; debe devolver `PSET2_USER`, `PSET2_ROLE` y `PSET2_WH`.
2. `ingesta_merma_raw.yml`: el flujo de ingesta.

> Si se usa otra cuenta o repositorio, actualizar las variables `sf_url` y `url_fuente` del flujo.

### 3.2 Cómo funciona la ingesta

| Aspecto | Configuración |
|---|---|
| Fuente | CSV en GitHub (`data/fuente_merma_levadura_2026.csv`), descargado por HTTP |
| Trigger | Quincenal: días 1 y 15 de cada mes a las 06:00 (`America/Guayaquil`) |
| Carga | Ventana de reproceso de **6 semanas** por `fecha_coccion`: `DELETE` + `INSERT` de la ventana (idempotente, sin duplicados) |
| Reintentos | 3 intentos, cada 5 minutos |
| Concurrencia | Una ejecución a la vez (`limit: 1`, en cola) para no pisar la tabla de aterrizaje |
| Backfill | Pestaña **Triggers** → `quincenal` → **Backfill executions** desde `2026-01-01` |

### 3.3 Backfill histórico

1. En el flujo `ingesta_merma_raw`: **Triggers → Backfill executions**.
2. Inicio: `2026-01-01 00:00`; fin: fecha actual.
3. Verificar en Snowflake:

```sql
SELECT COUNT(*) FROM CERVECERIA_DB.RAW.MERMA_LEVADURA;          -- 247
SELECT numero_documento, COUNT(*) FROM CERVECERIA_DB.RAW.MERMA_LEVADURA
GROUP BY 1 HAVING COUNT(*) > 1;                                  -- 0 filas (sin duplicados)
```

---

## 4. Transformaciones con dbt (RAW → BRONZE → SILVER → GOLD)

```bash
docker compose run --rm dbt debug     # verifica la conexión ("All checks passed!")
docker compose run --rm dbt run       # construye los 5 modelos
docker compose run --rm dbt test      # ejecuta todos los tests
```

> Si dbt no detecta un archivo nuevo, limpiar la caché: `docker compose run --rm dbt test --no-partial-parse`.

| Modelo | Capa | Qué hace |
|---|---|---|
| `stg_merma_levadura` | BRONZE | Lee de `source('raw', 'merma_levadura')` y convierte tipos con `try_to_*` |
| `int_merma_limpia` | SILVER | Elimina órdenes abiertas, productos sin levadura y errores de registro; anula datos no confiables; calcula `merma_pct` |
| `dim_estilo` | GOLD | Un estilo por fila con su cepa y tipo de levadura |
| `dim_fecha` | GOLD | Calendario diario 2025–2027 |
| `fct_lotes` | GOLD | Tabla de hechos. **Grano: una fila por orden de producción** |

**Tests incluidos:** `unique` y `not_null` en llaves, `accepted_values` en cepa, `relationships` de `fct_lotes` hacia `dim_estilo` y `dim_fecha`, y el test de negocio `tests/assert_merma_en_rango.sql` (merma entre 0 % y 100 %).

---

## 5. OBT con Spark (GOLD → OBT)

```bash
docker compose run --rm spark
```

La primera ejecución tarda unos minutos porque descarga Spark y el conector de Snowflake. El script `spark/build_obt.py`:

1. Lee `FCT_LOTES`, `DIM_ESTILO` y `DIM_FECHA` desde GOLD.
2. Une los hechos con las dimensiones (`dim_fecha` dos veces: cocción y cierre).
3. **Valida antes de escribir**: mismo número de filas que `fct_lotes`, órdenes únicas y sin dimensiones faltantes. Si algo falla, no escribe y termina con error.
4. Guarda el resultado en `CERVECERIA_DB.OBT.OBT_MERMA_LOTES`.

```sql
SELECT COUNT(*) FROM CERVECERIA_DB.OBT.OBT_MERMA_LOTES;   -- 221
```

---

## 6. Ejecución completa (resumen)

```bash
docker compose up -d                  # infraestructura
# Kestra: ejecutar ingesta_merma_raw (o esperar el trigger quincenal)
docker compose run --rm dbt run
docker compose run --rm dbt test
docker compose run --rm spark
```

---

## Seguridad

- No se suben contraseñas, tokens ni llaves: `.env`, `keys/` y los archivos generados por dbt están en `.gitignore`.
- `profiles.yml` y los flujos de Kestra leen las credenciales de variables de entorno y secretos.
- El usuario `PSET2_USER` es de tipo `SERVICE` y solo tiene permisos sobre `CERVECERIA_DB` y `PSET2_WH`.
