WITH age_filtered AS (
        SELECT client_id,
                state,
                DATE_DIFF(CURRENT_DATE(), birth_date, YEAR) AS age
        FROM `de-07-nazar-klypych.gold.user_profiles_enriched`
        WHERE DATE_DIFF(CURRENT_DATE(), birth_date, YEAR) BETWEEN 20 AND 30
    ),
     tv_sales AS (
        SELECT purchase_date,
                client_id
        FROM `de-07-nazar-klypych.silver.sales`
        WHERE product_name = 'TV'
            AND EXTRACT(MONTH FROM purchase_date) = 9
            AND EXTRACT(DAY FROM purchase_date) BETWEEN 1 AND 10
    ),
     tv_sales_by_state AS (
        SELECT a.state,
                COUNT(*) tv_count
        FROM age_filtered a
        JOIN tv_sales t
            ON a.client_id = t.client_id
        GROUP BY a.state
    )
SELECT *
FROM tv_sales_by_state
ORDER BY tv_count DESC
LIMIT 1;

-- IOWA - 179 tv sold