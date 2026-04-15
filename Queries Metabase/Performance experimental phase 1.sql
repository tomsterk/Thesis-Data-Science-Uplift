WITH customers_campaign AS (
    SELECT
        user_id AS customer_nk,
        file_name AS churn_group
    FROM ext_uploads.file_user_ids_uplift_20260212125800
    WHERE user_id IN (
        SELECT DISTINCT customer_nk
        FROM odl.fact_basket_items fbi
        LEFT JOIN odl.dim_customers dc USING (customer_sk)
        WHERE date_trading_nk < '2026-02-12'
    )
),
churned AS (
    SELECT
        dc.customer_nk,
        SUM(price_total_excl_vat) AS total_sales,
        SUM(margin_inc_retro) AS margin,
        COUNT(DISTINCT dc.customer_sk) AS reactivated
    FROM odl.fact_basket_items fbi
    JOIN odl.dim_customers dc USING (customer_sk)
    WHERE fbi.country_sk IN ('hbi|eu|nl', 'hbi|eu|be')
      AND fbi.date_trading_nk >= '2026-02-12'
      AND fbi.date_trading_nk <= '2026-03-12'
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