-- Tabla de hechos. Grano: una fila por orden de producción (lote)
select
    -- llave y referencias a dimensiones
    numero_documento,
    lote,
    fecha_coccion,          -- → dim_fecha
    fecha_cierre,           -- → dim_fecha
    estilo,                 -- → dim_estilo

    -- levadura
    generacion_levadura,
    generacion_levadura = 0                        as levadura_nueva,
    levadura_origen,

    -- microscopio
    con_microscopio,
    micro_viabilidad_pct,
    micro_celulas_viables_m_ml,
    micro_litros_levadura,
    micro_decision,

    -- medidas
    cantidad_planificada,
    cantidad_completada,
    n_cocciones,
    datediff(day, fecha_coccion, fecha_cierre)     as dias_proceso,
    merma_litros,
    merma_pct,

    -- banderas de calidad
    sin_generacion,
    generacion_con_conflicto,
    merma_ajustada,
    micro_origen_coincide
from {{ ref('int_merma_limpia') }}