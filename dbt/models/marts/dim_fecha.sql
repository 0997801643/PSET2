-- Dimensión de fecha: calendario diario 2025-2027 (cubre todas las cocciones y cierres)
with dias as (
    select dateadd(day, seq4(), '2025-01-01'::date) as fecha
    from table(generator(rowcount => 1095))
)
select
    fecha,
    year(fecha)                     as anio,
    month(fecha)                    as mes,
    monthname(fecha)                as nombre_mes,
    quarter(fecha)                  as trimestre,
    weekofyear(fecha)               as semana_anio,
    dayofweekiso(fecha)             as dia_semana,   -- 1 = lunes, 7 = domingo
    dayofweekiso(fecha) in (6, 7)   as es_fin_de_semana
from dias
