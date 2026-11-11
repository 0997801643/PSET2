-- Dimensión de estilo: un estilo por fila, con su cepa y tipo de levadura
select distinct
    estilo,
    cepa,
    tipo_levadura
from {{ ref('int_merma_limpia') }}
