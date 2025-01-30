WITH unique_first_visits AS (
    SELECT 
        user_pseudo_id AS user_id,
        DATE_TRUNC(MIN(PARSE_DATE('%Y%m%d', event_date)), WEEK) AS registration_week
    FROM `tc-da-1.turing_data_analytics.raw_events`
    GROUP BY user_pseudo_id
),

purchase_events AS (
    SELECT 
        user_pseudo_id AS user_id, 
        PARSE_DATE('%Y%m%d', event_date) AS purchase_date, 
        purchase_revenue_in_usd AS purchase_revenue
    FROM `tc-da-1.turing_data_analytics.raw_events`
    WHERE event_name = 'purchase' AND purchase_revenue_in_usd > 0
),

cohort_data_combined AS (
    SELECT 
        visits.user_id AS visitor_user_id, 
        visits.registration_week AS registration_week, 
        DATE_TRUNC(purchases.purchase_date, WEEK) AS purchase_week,
        purchases.purchase_revenue AS purchase_revenue,
        DATE_DIFF(DATE_TRUNC(purchases.purchase_date, WEEK), visits.registration_week, WEEK) AS week_difference
    FROM unique_first_visits visits
    LEFT JOIN purchase_events purchases ON visits.user_id = purchases.user_id
),

cohort_revenue_table AS (
    SELECT 
        registration_week,
        COUNT(DISTINCT visitor_user_id) AS total_registrations,
        SUM(IF(week_difference = 0, purchase_revenue, 0)) AS week_0,
        SUM(IF(week_difference = 1, purchase_revenue, 0)) AS week_1,
        SUM(IF(week_difference = 2, purchase_revenue, 0)) AS week_2,
        SUM(IF(week_difference = 3, purchase_revenue, 0)) AS week_3,
        SUM(IF(week_difference = 4, purchase_revenue, 0)) AS week_4,
        SUM(IF(week_difference = 5, purchase_revenue, 0)) AS week_5,
        SUM(IF(week_difference = 6, purchase_revenue, 0)) AS week_6,
        SUM(IF(week_difference = 7, purchase_revenue, 0)) AS week_7,
        SUM(IF(week_difference = 8, purchase_revenue, 0)) AS week_8,
        SUM(IF(week_difference = 9, purchase_revenue, 0)) AS week_9,
        SUM(IF(week_difference = 10, purchase_revenue, 0)) AS week_10,
        SUM(IF(week_difference = 11, purchase_revenue, 0)) AS week_11,
        SUM(IF(week_difference = 12, purchase_revenue, 0)) AS week_12
    FROM cohort_data_combined
    GROUP BY registration_week
)

-- Final result
SELECT 
    registration_week,
    total_registrations,
    week_0, week_1, week_2, week_3, week_4, 
    week_5, week_6, week_7, week_8, week_9, 
    week_10, week_11, week_12
FROM cohort_revenue_table
ORDER BY registration_week;