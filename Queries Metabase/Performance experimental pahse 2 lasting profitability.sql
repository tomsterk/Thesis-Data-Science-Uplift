WITH customers_campaign AS (
  SELECT
    customer_nk,
    model || ' - ' || original_incentive_name AS churn_group
  FROM (
    SELECT *, 'Binary Uplift control:' AS model
    FROM ext_uploads.control_selected_binary_2_20260323075455
    UNION ALL
    SELECT *, 'MTUM control:' AS model
    FROM ext_uploads.control_selected_multi_2_txt_20260324184208
    UNION ALL
    SELECT *, 'Binary Uplift' AS model
    FROM ext_uploads.treatment_selected_binary_20260318150232
    UNION ALL
    SELECT *, 'MTUM' AS model
    FROM ext_uploads.treatment_selected_multi_20260318150245
  )
  WHERE customer_nk IN (
    SELECT DISTINCT user_id
    FROM ext_uploads.correction_crm_users_20260320095100
  )
),
churned AS (
  SELECT
    dc.customer_nk,
    SUM(price_total_excl_vat) AS total_sales,
    SUM(margin_inc_retro) AS margin,
    COUNT(DISTINCT customer_sk) AS reactivated
  FROM odl.fact_basket_items fbi
  JOIN odl.dim_customers dc USING (customer_sk)
  WHERE fbi.country_sk IN ('hbi|eu|nl', 'hbi|eu|be')
    AND fbi.date_trading_nk >= '2026-03-19'
    AND fbi.date_trading_nk <= '2026-06-16'
  GROUP BY dc.customer_nk
)
SELECT
  c.customer_nk,
  c.churn_group,
  COALESCE(ch.reactivated, 0) AS reactivated,
  COALESCE(ch.total_sales, 0) AS total_sales,
  COALESCE(ch.margin, 0) AS margin
FROM customers_campaign c
LEFT JOIN churned ch
  ON ch.customer_nk = c.customer_nk
ORDER BY c.churn_group, c.customer_nk;