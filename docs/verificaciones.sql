-- =====================================================================
-- Verificaciones del pipeline, capa por capa.
-- Ejecutar después de cada etapa. Valores esperados en los comentarios.
-- =====================================================================

-- ---------- Configuración ----------
SHOW SCHEMAS IN DATABASE CERVECERIA_DB;      -- RAW, BRONZE, SILVER, GOLD, OBT
DESC USER PSET2_USER;                        -- RSA_PUBLIC_KEY_FP debe empezar con SHA256:

-- ---------- RAW (Kestra) ----------
SELECT COUNT(*) FROM CERVECERIA_DB.RAW.MERMA_LEVADURA;          -- 247

-- Idempotencia: ninguna orden duplicada tras el backfill (debe devolver 0 filas)
SELECT numero_documento, COUNT(*)
FROM CERVECERIA_DB.RAW.MERMA_LEVADURA
GROUP BY numero_documento
HAVING COUNT(*) > 1;

-- Lotes que aún no entran por estar fuera de la última ventana (debe ser 0 con la fuente actual)
SELECT COUNT(*)
FROM CERVECERIA_DB.RAW.MERMA_LEVADURA_LANDING
WHERE TRY_TO_DATE(fecha_coccion) >= '2026-11-01';

-- ---------- BRONZE (dbt staging) ----------
SELECT COUNT(*) FROM CERVECERIA_DB.BRONZE.STG_MERMA_LEVADURA;   -- 247 (staging no quita filas)

-- ---------- SILVER (dbt intermedia) ----------
-- 221 órdenes, 31 sin generación, 2 con merma ajustada
SELECT COUNT(*), COUNT_IF(sin_generacion), COUNT_IF(merma_ajustada)
FROM CERVECERIA_DB.SILVER.INT_MERMA_LIMPIA;

-- ---------- GOLD (dbt marts) ----------
SELECT COUNT(*) FROM CERVECERIA_DB.GOLD.FCT_LOTES;              -- 221
SELECT * FROM CERVECERIA_DB.GOLD.DIM_ESTILO;                    -- 9 estilos

-- ---------- OBT (Spark) ----------
SELECT COUNT(*) FROM CERVECERIA_DB.OBT.OBT_MERMA_LOTES;         -- 221

-- ---------- Reproceso (usar solo para repetir el backfill desde cero) ----------
-- TRUNCATE TABLE CERVECERIA_DB.RAW.MERMA_LEVADURA;
