WITH period_config AS (
  SELECT
    TO_DATE('{execution_start_date}', 'DD-MM-YYYY') AS execution_start_date,
    TO_DATE('{execution_end_date}', 'DD-MM-YYYY') AS execution_end_date
  FROM
    DUAL
),
period_with_format AS (
  SELECT
    execution_start_date,
    execution_end_date,
    TO_CHAR(execution_start_date, 'MM-YYYY') AS month_faturacao_format
  FROM
    period_config
),
billing_invoicing_data AS (
  SELECT
    CASE
      WHEN SEGMENTO = 'Fixo' THEN '101'
      WHEN SEGMENTO = 'Circuitos'
      OR SEGMENTO = 'Circuito' THEN '102'
      WHEN SEGMENTO = 'Movel' THEN '201'
      WHEN SEGMENTO = 'Pessoal' THEN '401'
      WHEN SEGMENTO = 'IPTV'
      OR SEGMENTO = 'TV' THEN '301'
      WHEN SEGMENTO = 'Internet' THEN '302'
      WHEN SEGMENTO = 'Pacotes'
      OR SEGMENTO = 'Pacote' THEN '303'
      WHEN SEGMENTO = 'Residencial' THEN '402'
      ELSE '999'
    END AS segment,
    CASE
      WHEN SEGMENTO IN ('Fixo', 'Circuitos', 'Circuito') THEN 'CVT'
      WHEN SEGMENTO IN ('Movel', 'Pessoal') THEN 'CVM'
      WHEN SEGMENTO IN (
        'IPTV',
        'TV',
        'Internet',
        'Pacotes',
        'Pacote',
        'Residencial'
      ) THEN 'CVMM'
      ELSE 'UNKNOWN'
    END AS company,
    UPPER(TIPO) AS process,
    TO_DATE(INICIO, 'DD-MM-YYYY HH24:MI:SS') AS init_dt,
    CASE
      WHEN FIM IS NOT NULL
      AND LENGTH(TRIM(FIM)) > 0 THEN TO_DATE(FIM, 'DD-MM-YYYY HH24:MI:SS')
      ELSE NULL
    END AS end_dt,
    CASE
      WHEN FIM IS NOT NULL
      AND LENGTH(TRIM(FIM)) > 0 THEN ROUND(
        (
          TO_DATE(FIM, 'DD-MM-YYYY HH24:MI:SS') - TO_DATE(INICIO, 'DD-MM-YYYY HH24:MI:SS')
        ) * 24 * 60
      )
      ELSE NULL
    END AS duration_minutes,
    CASE
      WHEN FIM IS NOT NULL
      AND LENGTH(TRIM(FIM)) > 0 THEN LPAD(
        FLOOR(
          ROUND(
            (
              TO_DATE(FIM, 'DD-MM-YYYY HH24:MI:SS') - TO_DATE(INICIO, 'DD-MM-YYYY HH24:MI:SS')
            ) * 24 * 60
          ) / 60
        ),
        2,
        '0'
      ) || ':' || LPAD(
        MOD(
          ROUND(
            (
              TO_DATE(FIM, 'DD-MM-YYYY HH24:MI:SS') - TO_DATE(INICIO, 'DD-MM-YYYY HH24:MI:SS')
            ) * 24 * 60
          ),
          60
        ),
        2,
        '0'
      ) || ':00'
      ELSE NULL
    END AS duration_hhmmss,
    CASE
      WHEN FIM IS NOT NULL
      AND LENGTH(TRIM(FIM)) > 0 THEN 'Completed'
      ELSE 'Running'
    END AS status,
    pc.month_faturacao_format AS month_faturacao
  FROM
    DE_SBILL_PROCESSES p
    CROSS JOIN period_with_format pc
  WHERE
    p.TIPO IN ('Billing', 'Invoicing')
    AND TO_DATE(p.INICIO, 'DD-MM-YYYY HH24:MI:SS') >= pc.execution_start_date
    AND TO_DATE(p.INICIO, 'DD-MM-YYYY HH24:MI:SS') <= pc.execution_end_date
),
billing_with_rn AS (
  SELECT
    segment,
    company,
    process,
    init_dt,
    end_dt,
    duration_minutes,
    duration_hhmmss,
    status,
    month_faturacao,
    ROW_NUMBER() OVER (
      PARTITION BY segment,
      process
      ORDER BY
        init_dt DESC
    ) AS rn
  FROM
    billing_invoicing_data
),
relatorio_base AS (
  SELECT
    REGEXP_SUBSTR(
      MSG_ERR,
      'Billing Segment = ([0-9]+)',
      1,
      1,
      '',
      1
    ) AS segment,
    CASE
      WHEN REGEXP_SUBSTR(
        MSG_ERR,
        'Billing Segment = ([0-9]+)',
        1,
        1,
        '',
        1
      ) IN ('101', '102') THEN 'CVT'
      WHEN REGEXP_SUBSTR(
        MSG_ERR,
        'Billing Segment = ([0-9]+)',
        1,
        1,
        '',
        1
      ) IN ('201', '401') THEN 'CVM'
      WHEN REGEXP_SUBSTR(
        MSG_ERR,
        'Billing Segment = ([0-9]+)',
        1,
        1,
        '',
        1
      ) IN ('301', '302', '303', '402') THEN 'CVMM'
      ELSE 'UNKNOWN'
    END AS company,
    BEGIN_DATE AS init_dt,
    END_DATE AS end_dt,
    CASE
      WHEN END_DATE IS NOT NULL THEN ROUND((END_DATE - BEGIN_DATE) * 24 * 60)
      ELSE NULL
    END AS duration_minutes,
    CASE
      WHEN END_DATE IS NOT NULL THEN LPAD(
        FLOOR(ROUND((END_DATE - BEGIN_DATE) * 24 * 60) / 60),
        2,
        '0'
      ) || ':' || LPAD(
        MOD(ROUND((END_DATE - BEGIN_DATE) * 24 * 60), 60),
        2,
        '0'
      ) || ':00'
      ELSE NULL
    END AS duration_hhmmss,
    CASE
      WHEN END_DATE IS NOT NULL THEN 'Completed'
      ELSE 'Running'
    END AS status,
    pc.month_faturacao_format AS month_faturacao
  FROM
    cst_processes_t r
    CROSS JOIN period_with_format pc
  WHERE
    r.ID_PROCESS_TYPE = 17
    AND r.MSG_ERR LIKE '%Billing Segment%'
    AND r.BEGIN_DATE >= pc.execution_start_date
    AND r.BEGIN_DATE <= pc.execution_end_date
),
relatorio_with_phase AS (
  SELECT
    segment,
    company,
    init_dt,
    end_dt,
    duration_minutes,
    duration_hhmmss,
    status,
    month_faturacao,
    CASE
      WHEN init_dt = MIN(init_dt) OVER (PARTITION BY segment) THEN 'RELATORIO PROVISORIO'
      WHEN init_dt = MAX(init_dt) OVER (PARTITION BY segment) THEN 'RELATORIO DEFINITIVA'
      ELSE 'SKIP'
    END AS process
  FROM
    relatorio_base
),
relatorio_with_rn AS (
  SELECT
    segment,
    company,
    process,
    init_dt,
    end_dt,
    duration_minutes,
    duration_hhmmss,
    status,
    month_faturacao,
    ROW_NUMBER() OVER (
      PARTITION BY segment,
      process
      ORDER BY
        init_dt DESC
    ) AS rn
  FROM
    relatorio_with_phase
  WHERE
    process != 'SKIP'
),
deduplicated_billing AS (
  SELECT
    segment,
    company,
    process,
    init_dt,
    end_dt,
    duration_minutes,
    duration_hhmmss,
    status,
    month_faturacao
  FROM
    billing_with_rn
  WHERE
    rn = 1
    AND company != 'UNKNOWN'
),
filtered_relatorio AS (
  SELECT
    segment,
    company,
    process,
    init_dt,
    end_dt,
    duration_minutes,
    duration_hhmmss,
    status,
    month_faturacao
  FROM
    relatorio_with_rn
  WHERE
    rn = 1
    AND company != 'UNKNOWN'
)
SELECT
  segment,
  company,
  process,
  init_dt,
  end_dt,
  duration_minutes AS DURATION_MN,
  duration_hhmmss,
  status,
  month_faturacao
FROM
  deduplicated_billing
UNION
ALL
SELECT
  segment,
  company,
  process,
  init_dt,
  end_dt,
  duration_minutes AS DURATION_MN,
  duration_hhmmss,
  status,
  month_faturacao
FROM
  filtered_relatorio
ORDER BY
  company,
  segment,
  init_dt