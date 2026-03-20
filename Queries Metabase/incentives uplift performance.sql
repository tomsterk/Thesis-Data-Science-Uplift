WITH
  customers_campaign AS (
    /* Customers that received incentive or control group */
    SELECT
      file_name AS churn_group,
      user_id AS customer_nk
    FROM
      ext_uploads.file_user_ids_uplift_20260212125800 ec
  ),
  churned AS (
    SELECT
      dc.customer_nk,
      1 AS did_txn_last_12m,
      sum(price_total_excl_vat) total_sales,
      sum(margin_inc_retro) margin,
      count(DISTINCT customer_sk) reactivated
    FROM
      odl.fact_basket_items fbi
      JOIN odl.dim_customers dc USING (customer_sk)
    WHERE
      fbi.country_sk IN ('hbi|eu|nl', 'hbi|eu|be')
      AND fbi.date_trading_nk >= '2026-02-12'
      AND fbi.date_trading_nk <= '2026-03-12'
    GROUP BY
      dc.customer_nk
  )
SELECT
  c.churn_group AS treatment_indicator,
  count(DISTINCT c.customer_nk),
  sum(total_sales) total_sales,
  sum(margin) total_margin,
  sum(reactivated) AS reactivated,
  sum(total_sales) / count(DISTINCT c.customer_nk) sales_per_cust,
  sum(total_sales) / sum(reactivated) sales_per_reactivated
FROM
  customers_campaign c
  LEFT JOIN churned ch ON ch.customer_nk = c.customer_nk
  -- Customers with transaction before 
WHERE
  c.customer_nk IN (
    SELECT DISTINCT
      customer_nk
    FROM
      odl.fact_basket_items fbi
      LEFT JOIN odl.dim_customers USING (customer_sk)
    WHERE
      date_trading_nk < '2026-02-12'
  )
GROUP BY
  1
ORDER BY
  3 DESC