-- Capa intermedia (SILVER): aplica las decisiones de limpieza documentadas.
with base as (
    select * from {{ ref('stg_merma_levadura') }}
),

filtrado as (
    select *
    from base
    where
        -- Órdenes abiertas: sin merma conocida, no sirven para entrenar
        fecha_cierre is not null
        and cantidad_completada > 0
        -- Productos sin levadura (agua tónica, cóctel): fuera del alcance
        and estilo not in ('Platonic', 'Turbo + Guarana Tropical')
        -- Completado > 105% del planificado (orden 1259): error de tipeo no confirmable
        and cantidad_completada <= cantidad_planificada * 1.05
),

limpio as (
    select
        numero_documento,
        lote,
        fecha_coccion,
        fecha_cierre,
        estilo,
        cepa,
        tipo_levadura,

        -- Generación: si las hojas de la bitácora no coinciden, no es confiable
        case when generacion_con_conflicto then null
             else generacion_levadura end              as generacion_levadura,
        levadura_origen,

        -- Microscopio: se conserva la viabilidad; el conteo dudoso se anula
        con_microscopio,
        micro_viabilidad_pct,
        case when micro_dato_dudoso then null
             else micro_celulas_viables_m_ml end      as micro_celulas_viables_m_ml,
        micro_litros_levadura,
        micro_decision,

        cantidad_planificada,
        -- Merma negativa pequeña (mezcla de lotes): se ajusta a merma 0
        least(cantidad_completada, cantidad_planificada) as cantidad_completada,
        n_cocciones,

        -- Banderas de calidad
        generacion_con_conflicto,
        micro_origen_coincide,
        micro_dato_dudoso,
        cantidad_completada > cantidad_planificada    as merma_ajustada
    from filtrado
)

select
    *,
    generacion_levadura is null                       as sin_generacion,
    cantidad_planificada - cantidad_completada        as merma_litros,
    round((cantidad_planificada - cantidad_completada)
          / cantidad_planificada * 100, 2)            as merma_pct
from limpio