/* 
PORTFOLIO NOTE:
This SQL is a simulated/anonymized version for portfolio purposes.
All schema/table names and identifiers were modified to protect sensitive information.
No real data is included.
*/

WITH params AS (
  SELECT
    1::bigint      AS p_load_id,
    0.01::numeric  AS tol_montos,
    0::int         AS tol_dias
),
src AS (
  SELECT
    lf.load_id,
    lf.linea,
    lf.valor::jsonb AS js
  FROM staging.load_fields lf
  JOIN params p ON p.p_load_id = lf.load_id
  WHERE lf.estado IS NULL OR lf.estado = 1
),
manual AS (
  SELECT
    src.load_id,
    src.linea,
    NULLIF(TRIM(BOTH FROM src.js ->> 'Tipo de documento de identificación del titular'), '') AS tit_tipo_id,
    NULLIF(TRIM(BOTH FROM src.js ->> 'Número de documento de identificación del titular'), '') AS tit_id,
    NULLIF(TRIM(BOTH FROM src.js ->> 'Tipo de documento de identificación del Beneficiario'), '') AS ben_tipo_id,
    NULLIF(TRIM(BOTH FROM src.js ->> 'Número de documento de identificación del Beneficiario'), '') AS ben_id,
    NULLIF(TRIM(BOTH FROM src.js ->> 'Grupo etario del Beneficiario'), '') AS nov_grupo_etario,
    NULLIF(TRIM(BOTH FROM src.js ->> 'Tipo de Novedad'), '') AS nov_tipo,
    NULLIF(TRIM(BOTH FROM src.js ->> 'Días cobrados'), '')::numeric AS nov_dias_cobrados,
    NULLIF(TRIM(BOTH FROM src.js ->> 'Valor de la prima diaria'), '')::numeric AS nov_valor_prima_dia,
    NULLIF(TRIM(BOTH FROM src.js ->> 'Valor de la Novedad'), '')::numeric AS nov_valor_prorrateo,
    NULLIF(TRIM(BOTH FROM src.js ->> 'Descuento de Fidelidad'), '')::numeric AS nov_descuento,
    NULLIF(TRIM(BOTH FROM src.js ->> 'Valor de la novedad con descuento'), '')::numeric AS nov_valor_con_dcto,
    NULLIF(TRIM(BOTH FROM src.js ->> 'Valor IVA'), '')::numeric AS nov_iva,
    NULLIF(TRIM(BOTH FROM src.js ->> 'Valor Neto de la Novedad'), '')::numeric AS nov_valor_neto,
    NULLIF(TRIM(BOTH FROM src.js ->> 'Fecha inicio periodo de la novedad'), '')::date AS nov_fec_ini_periodo,
    NULLIF(TRIM(BOTH FROM src.js ->> 'Fecha fin periodo de la novedad'), '')::date AS nov_fec_fin_periodo,
    NULLIF(TRIM(BOTH FROM src.js ->> 'Fecha inicio de periodo'), '')::date AS nov_fec_ini_facturacion,
    NULLIF(TRIM(BOTH FROM src.js ->> 'Fecha fin de periodo'), '')::date AS nov_fec_fin_facturacion,
    NULLIF(TRIM(BOTH FROM src.js ->> 'Código de usuario'), '') AS cod_usuario,
    NULLIF(TRIM(BOTH FROM src.js ->> 'Código DANE Departamento'), '') AS ser_dpto,
    NULLIF(TRIM(BOTH FROM src.js ->> 'Código DANE Ciudad'), '') AS ser_ciudad
  FROM src
  WHERE (src.js ->> 'Código de usuario') IS NOT NULL
    AND (src.js ->> 'Código de usuario') <> 'Código de usuario'
),
periodo AS (
  SELECT
    m.load_id,
    m.linea,
    m.tit_tipo_id,
    m.tit_id,
    m.ben_tipo_id,
    m.ben_id,
    m.nov_grupo_etario,
    m.nov_tipo,
    m.nov_dias_cobrados,
    m.nov_valor_prima_dia,
    m.nov_valor_prorrateo,
    m.nov_descuento,
    m.nov_valor_con_dcto,
    m.nov_iva,
    m.nov_valor_neto,
    m.nov_fec_ini_periodo,
    m.nov_fec_fin_periodo,
    m.nov_fec_ini_facturacion,
    m.nov_fec_fin_facturacion,
    m.cod_usuario,
    m.ser_dpto,
    m.ser_ciudad,
    EXTRACT(year  FROM m.nov_fec_ini_facturacion)::integer  AS anio,
    EXTRACT(month FROM m.nov_fec_ini_facturacion)::integer  AS mes
  FROM manual m
),
dup AS (
  SELECT
    periodo.tit_tipo_id,
    periodo.tit_id,
    periodo.anio,
    periodo.mes,
    periodo.nov_tipo,
    count(*) AS cnt
  FROM periodo
  GROUP BY periodo.tit_tipo_id, periodo.tit_id, periodo.anio, periodo.mes, periodo.nov_tipo
),
base_inicial_raw AS (
  SELECT
    TRIM(BOTH FROM lf.valor ->> 'CODIGO')        AS cod_usuario,
    TRIM(BOTH FROM lf.valor ->> 'COD_TIP_ID_BEN') AS ben_tipo_id_base,
    TRIM(BOTH FROM lf.valor ->> 'ID_BEN')        AS ben_id_base,
    l.load_id
  FROM staging.load_fields lf
  JOIN staging.loads l ON l.load_id = lf.load_id
  WHERE l.nombre_archivo::text = 'initial_base.csv'
    AND l.estado = 1
    AND NULLIF(TRIM(BOTH FROM lf.valor ->> 'CODIGO'), '') IS NOT NULL
),
base_ben AS (
  SELECT DISTINCT ON (base_inicial_raw.cod_usuario)
    base_inicial_raw.cod_usuario,
    NULLIF(base_inicial_raw.ben_tipo_id_base, '') AS ben_tipo_id_base,
    NULLIF(base_inicial_raw.ben_id_base, '')      AS ben_id_base
  FROM base_inicial_raw
  ORDER BY base_inicial_raw.cod_usuario, base_inicial_raw.load_id DESC
),
vm AS (
  SELECT
    TRIM(BOTH FROM mv.codigo) AS cod_usuario,
    NULLIF(TRIM(BOTH FROM mv.cod_tip_id_ben), '') AS ben_tipo_id_mv,
    NULLIF(TRIM(BOTH FROM mv.id_ben), '')         AS ben_id_mv,
    mv.dias_laborados,
    mv.iva AS iva_pct_mv,
    mv.tipo_novedad AS tipo_novedad_diaria,
    mv.fecha_novedad,
    mv.justificacion
  FROM mart.affiliate_updates_mv mv
),
calc AS (
  SELECT
    p.load_id,
    p.linea,
    p.tit_tipo_id,
    p.tit_id,
    p.ben_tipo_id,
    p.ben_id,
    p.nov_grupo_etario,
    p.nov_tipo,
    p.nov_dias_cobrados,
    p.nov_valor_prima_dia,
    p.nov_valor_prorrateo,
    p.nov_descuento,
    p.nov_valor_con_dcto,
    p.nov_iva,
    p.nov_valor_neto,
    p.nov_fec_ini_periodo,
    p.nov_fec_fin_periodo,
    p.nov_fec_ini_facturacion,
    p.nov_fec_fin_facturacion,
    p.cod_usuario,
    p.ser_dpto,
    p.ser_ciudad,
    p.anio,
    p.mes,
    COALESCE(d.cnt, 0::bigint) AS cnt_duplicados,
    CASE WHEN v.cod_usuario IS NULL THEN 0 ELSE 1 END AS mv_encontrado,
    v.dias_laborados AS mv_dias_laborados,
    v.iva_pct_mv     AS mv_iva_pct,
    v.tipo_novedad_diaria,
    v.fecha_novedad,
    v.justificacion,
    COALESCE(v.ben_tipo_id_mv, b.ben_tipo_id_base, p.ben_tipo_id) AS ben_tipo_id_cross,
    COALESCE(v.ben_id_mv,     b.ben_id_base,      p.ben_id)      AS ben_id_cross,

    round(COALESCE(p.nov_dias_cobrados, 0::numeric) * COALESCE(p.nov_valor_prima_dia, 0::numeric), 2) AS calc_nov_valor_prorrateo,
    round(
      round(COALESCE(p.nov_dias_cobrados, 0::numeric) * COALESCE(p.nov_valor_prima_dia, 0::numeric), 2)
      + COALESCE(p.nov_descuento, 0::numeric),
      2
    ) AS calc_nov_valor_con_dcto,

    CASE
      WHEN COALESCE(v.iva_pct_mv, 0) = 0 THEN 0::numeric
      WHEN round(
             round(COALESCE(p.nov_dias_cobrados, 0::numeric) * COALESCE(p.nov_valor_prima_dia, 0::numeric), 2)
             + COALESCE(p.nov_descuento, 0::numeric),
             2
           ) = 0::numeric THEN 0::numeric
      WHEN round(
             round(COALESCE(p.nov_dias_cobrados, 0::numeric) * COALESCE(p.nov_valor_prima_dia, 0::numeric), 2)
             + COALESCE(p.nov_descuento, 0::numeric),
             2
           ) > 0::numeric
        THEN abs(
               round(
                 v.iva_pct_mv::numeric / 100.0
                 * round(
                     round(COALESCE(p.nov_dias_cobrados, 0::numeric) * COALESCE(p.nov_valor_prima_dia, 0::numeric), 2)
                     + COALESCE(p.nov_descuento, 0::numeric),
                     2
                   ),
                 2
               )
             )
      ELSE -abs(
              round(
                v.iva_pct_mv::numeric / 100.0
                * round(
                    round(COALESCE(p.nov_dias_cobrados, 0::numeric) * COALESCE(p.nov_valor_prima_dia, 0::numeric), 2)
                    + COALESCE(p.nov_descuento, 0::numeric),
                    2
                  ),
                2
              )
            )
    END AS calc_nov_iva,

    round(
      round(
        round(COALESCE(p.nov_dias_cobrados, 0::numeric) * COALESCE(p.nov_valor_prima_dia, 0::numeric), 2)
        + COALESCE(p.nov_descuento, 0::numeric),
        2
      )
      +
      CASE
        WHEN COALESCE(v.iva_pct_mv, 0) = 0 THEN 0::numeric
        WHEN round(
               round(COALESCE(p.nov_dias_cobrados, 0::numeric) * COALESCE(p.nov_valor_prima_dia, 0::numeric), 2)
               + COALESCE(p.nov_descuento, 0::numeric),
               2
             ) > 0::numeric
          THEN abs(
                 round(
                   v.iva_pct_mv::numeric / 100.0
                   * round(
                       round(COALESCE(p.nov_dias_cobrados, 0::numeric) * COALESCE(p.nov_valor_prima_dia, 0::numeric), 2)
                       + COALESCE(p.nov_descuento, 0::numeric),
                       2
                     ),
                   2
                 )
               )
        ELSE -abs(
                round(
                  v.iva_pct_mv::numeric / 100.0
                  * round(
                      round(COALESCE(p.nov_dias_cobrados, 0::numeric) * COALESCE(p.nov_valor_prima_dia, 0::numeric), 2)
                      + COALESCE(p.nov_descuento, 0::numeric),
                      2
                    ),
                  2
                )
              )
      END,
      2
    ) AS calc_nov_valor_neto
  FROM periodo p
  LEFT JOIN dup d
    ON d.tit_tipo_id = p.tit_tipo_id
   AND d.tit_id      = p.tit_id
   AND d.anio        = p.anio
   AND d.mes         = p.mes
   AND d.nov_tipo    = p.nov_tipo
  LEFT JOIN vm v
    ON TRIM(BOTH FROM v.cod_usuario) = TRIM(BOTH FROM p.cod_usuario)
  LEFT JOIN base_ben b
    ON TRIM(BOTH FROM b.cod_usuario) = TRIM(BOTH FROM p.cod_usuario)
)

SELECT
  c.tit_tipo_id,
  c.tit_id,
  c.ben_tipo_id,
  c.ben_id,
  c.nov_grupo_etario,
  c.nov_tipo,
  c.nov_dias_cobrados,
  c.nov_valor_prima_dia,
  c.nov_valor_prorrateo,
  c.nov_descuento,
  c.nov_valor_con_dcto,
  c.nov_iva,
  c.nov_valor_neto,
  c.nov_fec_ini_periodo,
  c.nov_fec_fin_periodo,
  c.nov_fec_ini_facturacion,
  c.nov_fec_fin_facturacion,
  c.cod_usuario,
  c.ser_dpto,
  c.ser_ciudad,

  c.load_id,
  c.linea,
  c.anio,
  c.mes,
  c.cnt_duplicados,

  c.calc_nov_valor_prorrateo,
  c.calc_nov_valor_con_dcto,
  c.calc_nov_iva,
  c.calc_nov_valor_neto,

  abs(COALESCE(c.nov_valor_prorrateo, 0::numeric) - COALESCE(c.calc_nov_valor_prorrateo, 0::numeric))
    <= (SELECT params.tol_montos FROM params) AS aud_arch_ok_9,

  abs(COALESCE(c.nov_valor_con_dcto, 0::numeric) - COALESCE(c.calc_nov_valor_con_dcto, 0::numeric))
    <= (SELECT params.tol_montos FROM params) AS aud_arch_ok_11,

  abs(COALESCE(c.nov_iva, 0::numeric) - COALESCE(c.calc_nov_iva, 0::numeric))
    <= (SELECT params.tol_montos FROM params) AS aud_arch_ok_12,

  abs(COALESCE(c.nov_valor_neto, 0::numeric) - COALESCE(c.calc_nov_valor_neto, 0::numeric))
    <= (SELECT params.tol_montos FROM params) AS aud_arch_ok_13,

  c.mv_encontrado,
  c.mv_dias_laborados,
  c.mv_iva_pct,
  c.tipo_novedad_diaria,
  c.fecha_novedad,
  c.justificacion,
  c.ben_tipo_id_cross,
  c.ben_id_cross,

  COALESCE(TRIM(BOTH FROM c.ben_tipo_id), '') = COALESCE(TRIM(BOTH FROM c.ben_tipo_id_cross), '')
  AND COALESCE(TRIM(BOTH FROM c.ben_id), '')  = COALESCE(TRIM(BOTH FROM c.ben_id_cross), '') AS aud_mv_ok_doc_ben,

  CASE
    WHEN c.tit_tipo_id IS NULL OR c.tit_id IS NULL THEN 'NO OK'
    WHEN c.ben_tipo_id IS NULL OR c.ben_id IS NULL THEN 'NO OK'
    WHEN c.nov_grupo_etario IS NULL OR c.nov_tipo IS NULL THEN 'NO OK'
    WHEN c.nov_dias_cobrados IS NULL OR c.nov_dias_cobrados < 0::numeric THEN 'NO OK'
    WHEN c.nov_valor_prima_dia IS NULL OR c.nov_valor_prima_dia <= 0::numeric THEN 'NO OK'
    WHEN c.nov_fec_ini_periodo IS NULL OR c.nov_fec_fin_periodo IS NULL THEN 'NO OK'
    WHEN c.nov_fec_ini_facturacion IS NULL OR c.nov_fec_fin_facturacion IS NULL THEN 'NO OK'
    WHEN c.nov_fec_ini_periodo > c.nov_fec_fin_periodo THEN 'NO OK'
    WHEN c.nov_fec_ini_facturacion > c.nov_fec_fin_facturacion THEN 'NO OK'
    WHEN c.nov_tipo <> ALL (ARRAY['AJE_POS','AJE_NEG','RET','AJE_INC']) THEN 'NO OK'
    WHEN c.ser_dpto !~ '^\d{2}$' OR c.ser_ciudad !~ '^\d{3}$' THEN 'NO OK'
    WHEN c.cnt_duplicados > 1 THEN 'NO OK'
    ELSE 'OK'
  END AS estado_validacion_manual

FROM calc c
ORDER BY
  (CASE
     WHEN c.tit_tipo_id IS NULL OR c.tit_id IS NULL THEN 'NO OK'
     WHEN c.ben_tipo_id IS NULL OR c.ben_id IS NULL THEN 'NO OK'
     WHEN c.nov_grupo_etario IS NULL OR c.nov_tipo IS NULL THEN 'NO OK'
     WHEN c.nov_dias_cobrados IS NULL OR c.nov_dias_cobrados < 0::numeric THEN 'NO OK'
     WHEN c.nov_valor_prima_dia IS NULL OR c.nov_valor_prima_dia <= 0::numeric THEN 'NO OK'
     WHEN c.nov_fec_ini_periodo IS NULL OR c.nov_fec_fin_periodo IS NULL THEN 'NO OK'
     WHEN c.nov_fec_ini_facturacion IS NULL OR c.nov_fec_fin_facturacion IS NULL THEN 'NO OK'
     WHEN c.nov_fec_ini_periodo > c.nov_fec_fin_periodo THEN 'NO OK'
     WHEN c.nov_fec_ini_facturacion > c.nov_fec_fin_facturacion THEN 'NO OK'
     WHEN c.nov_tipo <> ALL (ARRAY['AJE_POS','AJE_NEG','RET','AJE_INC']) THEN 'NO OK'
     WHEN c.ser_dpto !~ '^\d{2}$' OR c.ser_ciudad !~ '^\d{3}$' THEN 'NO OK'
     WHEN c.cnt_duplicados > 1 THEN 'NO OK'
     ELSE 'OK'
   END) DESC,
  c.mv_encontrado DESC,
  c.anio,
  c.mes,
  c.tit_tipo_id,
  c.tit_id,
  c.linea;
