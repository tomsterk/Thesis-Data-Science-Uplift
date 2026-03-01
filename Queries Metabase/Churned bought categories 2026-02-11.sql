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
  dcf.category_fpna_l3_name,
  COUNT(DISTINCT fbi.customer_sk) AS customers,
  SUM(fbi.price_total_excl_vat) AS price_total_excl_vat,
  SUM(fbi.quantity) AS quantity,
  SUM(fbi.margin_inc_retro) AS margin_inc_retro,
  COUNT(DISTINCT fbi.customer_sk) * 1.0 / (
    SELECT
      COUNT(DISTINCT dc.customer_sk)
    FROM
      odl.dim_customers dc
      JOIN purchase_flags pf ON dc.customer_sk = pf.customer_sk
    WHERE
      dc.email_marketing_flag = 1
      AND dc.gdpr_consent_flag = 1
      AND dc.country_sk IN ('hbi|eu|nl', 'hbi|eu|be')
      AND pf.has_recent_purchase = 0
  ) * 100 AS perc_of_total
FROM
  odl.fact_basket_items fbi
  LEFT JOIN odl.dim_products dp ON fbi.product_sk = dp.product_sk
  LEFT JOIN odl.dim_categories_fpna dcf ON dp.category_fpna_sk = dcf.category_fpna_sk
  JOIN purchase_flags pf ON fbi.customer_sk = pf.customer_sk
  JOIN odl.dim_customers dc ON fbi.customer_sk = dc.customer_sk
WHERE
  fbi.country_sk IN ('hbi|eu|nl', 'hbi|eu|be')
  AND dc.email_marketing_flag = 1
  AND dc.gdpr_consent_flag = 1
  AND dc.country_sk IN ('hbi|eu|nl', 'hbi|eu|be')
  AND pf.has_recent_purchase = 0
  AND date_trading_nk < '2026-02-12'
GROUP BY
  1
ORDER BY
  6 DESC;