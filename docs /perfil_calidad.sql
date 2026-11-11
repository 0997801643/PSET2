WITH s AS (SELECT * FROM CERVECERIA_DB.BRONZE.STG_MERMA_LEVADURA),
t AS (SELECT COUNT(*) AS total FROM s)
SELECT dimension, problema, filas, ROUND(100 * filas / total, 1) AS pct
FROM (
  SELECT 'Completitud' AS dimension, 'Sin generación de levadura' AS problema,
         COUNT_IF(generacion_levadura IS NULL) AS filas FROM s
  UNION ALL SELECT 'Completitud', 'Sin cantidad planificada', COUNT_IF(cantidad_planificada IS NULL) FROM s
  UNION ALL SELECT 'Completitud', 'Sin fecha de cierre (orden abierta)', COUNT_IF(fecha_cierre IS NULL) FROM s
  UNION ALL SELECT 'Completitud', 'Sin cepa', COUNT_IF(cepa IS NULL) FROM s
  UNION ALL SELECT 'Completitud', 'Periodo microscopio sin viabilidad',
         COUNT_IF(con_microscopio AND micro_viabilidad_pct IS NULL) FROM s
  UNION ALL SELECT 'Validez', 'Completado = 0', COUNT_IF(cantidad_completada = 0) FROM s
  UNION ALL SELECT 'Validez', 'Completado > planificado (merma negativa)',
         COUNT_IF(cantidad_completada > cantidad_planificada) FROM s
  UNION ALL SELECT 'Validez', 'Completado > 105% del planificado',
         COUNT_IF(cantidad_completada > cantidad_planificada * 1.05) FROM s
  UNION ALL SELECT 'Validez', 'No es cerveza (sin levadura)',
         COUNT_IF(estilo IN ('Platonic', 'Turbo + Guarana Tropical')) FROM s
  UNION ALL SELECT 'Consistencia', 'Generación distinta entre hojas de bitácora',
         COUNT_IF(generacion_con_conflicto) FROM s
  UNION ALL SELECT 'Consistencia', 'FV de origen no coincide con microscopio',
         COUNT_IF(micro_origen_coincide = FALSE) FROM s
  UNION ALL SELECT 'Precisión', 'Conteo de células dudoso', COUNT_IF(micro_dato_dudoso) FROM s
  UNION ALL SELECT 'Precisión', 'Merma > 20%',
         COUNT_IF((cantidad_planificada - cantidad_completada) / NULLIF(cantidad_planificada, 0) > 0.20
                  AND cantidad_completada > 0) FROM s
), t
ORDER BY dimension, filas DESC;
