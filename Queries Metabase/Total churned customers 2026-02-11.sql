WITH
  purchase_flags AS (
    SELECT
      fbi.customer_sk,
      MAX(
        CASE
          WHEN CAST(fbi.date_trading_nk AS DATE) >= DATEADD('day', -365, CAST('2026-02-12' AS DATE))
          AND CAST(fbi.date_trading_nk AS DATE) < CAST('2026-02-12' AS DATE) THEN 1
          ELSE 0
        END
      ) AS has_recent_purchase
    FROM
      odl.fact_basket_items fbi
    WHERE
      fbi.country_sk IN ('hbi|eu|nl', 'hbi|eu|be')
      AND date_trading_nk < '2026-02-12'
    GROUP BY
      fbi.customer_sk
  )
SELECT
  COUNT(DISTINCT dc.customer_sk) AS customer_count
FROM
  odl.dim_customers dc
  JOIN purchase_flags pf ON dc.customer_sk = pf.customer_sk
WHERE
  dc.email_marketing_flag = 1
  AND dc.gdpr_consent_flag = 1
  AND dc.country_sk IN ('hbi|eu|nl', 'hbi|eu|be')
  AND pf.has_recent_purchase = 0;