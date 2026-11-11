-- Staging: única capa que lee de RAW. Solo tipa y renombra, no filtra filas.
select
    -- identificadores
    try_to_number(numero_documento)          as numero_documento,
    lote,

    -- fechas
    try_to_date(fecha_coccion)               as fecha_coccion,
    try_to_date(fecha_cierre)                as fecha_cierre,

    -- producto y levadura
    estilo,
    cepa,
    tipo_levadura,
    try_to_number(generacion_levadura)       as generacion_levadura,
    try_to_boolean(levadura_nueva)           as levadura_nueva,
    levadura_origen,
    try_to_boolean(generacion_con_conflicto) as generacion_con_conflicto,

    -- microscopio
    try_to_boolean(con_microscopio)          as con_microscopio,
    try_to_double(micro_viabilidad_pct)      as micro_viabilidad_pct,
    try_to_double(micro_celulas_viables_m_ml) as micro_celulas_viables_m_ml,
    try_to_double(micro_litros_levadura)     as micro_litros_levadura,
    micro_decision,
    try_to_number(micro_n_muestras)          as micro_n_muestras,
    try_to_boolean(micro_origen_coincide)    as micro_origen_coincide,
    try_to_number(micro_gen_lodo)            as micro_gen_lodo,
    try_to_boolean(micro_dato_dudoso)        as micro_dato_dudoso,

    -- cantidades
    try_to_double(cantidad_planificada)      as cantidad_planificada,
    try_to_double(cantidad_completada)       as cantidad_completada,
    try_to_double(n_cocciones)               as n_cocciones,

    -- calendario
    try_to_number(mes)                       as mes,
    try_to_number(trimestre)                 as trimestre,
    try_to_number(dia_semana)                as dia_semana,
    try_to_number(dias_orden)                as dias_orden,

    -- metadatos de carga
    _archivo_origen,
    _fecha_carga

from {{ source('raw', 'merma_levadura') }}  
