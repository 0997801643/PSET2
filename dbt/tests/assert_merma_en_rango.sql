-- Regla de negocio: después de la limpieza, la merma debe estar entre 0% y 100%
select numero_documento, merma_pct
from {{ ref('int_merma_limpia') }}
where merma_pct < 0 or merma_pct > 100  