WITH CTE_History AS (
    SELECT
        ProcID,
        uf101,
        uf102,
        uf201,
        uf211,
        uf301,
        uf300,
        uf400,
        uf204,
        uf202,
        uf212,
        uf213,
        uf214,
        uf203
    FROM
        dbo.History{month_year}
    WHERE
        uf101 in (1, 2)
        AND ProcID LIKE 'CTL_HUAWEI_CS-%'
        AND uf301 = '{date_val}'
)
SELECT
    A.uf204,
    A.uf102,
    A.uf201,
    A.uf211,
    A.uf301,
    A.uf300,
    A.uf400,
    A.uf202,
    A.uf203,
    SUBSTRING(A.uf204, 1, 14) AS imei_14,
    A.ProcID,
    A.uf212,
    A.uf213,
    A.uf214,
    SUBSTRING(B.uf214, 1, 14) AS imei_14_destino
FROM
    CTE_History A
    LEFT JOIN CTE_History B ON A.uf201 = B.uf201
    AND A.uf211 = B.uf211
    AND A.uf300 = B.uf300
    AND A.uf301 = B.uf301
WHERE
    A.uf101 = 1
    AND B.uf101 = 2