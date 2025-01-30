WITH cohort_data AS (
  SELECT *,
    DATE_TRUNC(subscription_start, WEEK(SUNDAY)) AS subscription_start_week,
    DATE_TRUNC(subscription_end, WEEK(SUNDAY)) AS subscription_end_week
  FROM turing_data_analytics.subscriptions
),
user_counts AS (
  SELECT
    subscription_start_week,
    COUNT(DISTINCT user_pseudo_id) AS total_users
  FROM cohort_data
  GROUP BY 1
),
week_0_retention AS (
  SELECT
    subscription_start_week,
    COUNT(DISTINCT user_pseudo_id) AS week_0_retained_users
  FROM cohort_data
  WHERE subscription_start_week < subscription_end_week OR subscription_end_week IS NULL
  GROUP BY 1
),
week_1_retention AS (
  SELECT
    subscription_start_week,
    COUNT(DISTINCT user_pseudo_id) AS week_1_retained_users
  FROM cohort_data
  WHERE DATE_ADD(subscription_start_week, INTERVAL 1 WEEK) < subscription_end_week OR subscription_end_week IS NULL
  GROUP BY 1
),
week_2_retention AS (
  SELECT
    subscription_start_week,
    COUNT(DISTINCT user_pseudo_id) AS week_2_retained_users
  FROM cohort_data
  WHERE DATE_ADD(subscription_start_week, INTERVAL 2 WEEK) < subscription_end_week OR subscription_end_week IS NULL
  GROUP BY 1
),
week_3_retention AS (
  SELECT
    subscription_start_week,
    COUNT(DISTINCT user_pseudo_id) AS week_3_retained_users
  FROM cohort_data
  WHERE DATE_ADD(subscription_start_week, INTERVAL 3 WEEK) < subscription_end_week OR subscription_end_week IS NULL
  GROUP BY 1
),
week_4_retention AS (
  SELECT
    subscription_start_week,
    COUNT(DISTINCT user_pseudo_id) AS week_4_retained_users
  FROM cohort_data
  WHERE DATE_ADD(subscription_start_week, INTERVAL 4 WEEK) < subscription_end_week OR subscription_end_week IS NULL
  GROUP BY 1
),
week_5_retention AS (
  SELECT
    subscription_start_week,
    COUNT(DISTINCT user_pseudo_id) AS week_5_retained_users
  FROM cohort_data
  WHERE DATE_ADD(subscription_start_week, INTERVAL 5 WEEK) < subscription_end_week OR subscription_end_week IS NULL
  GROUP BY 1
),
week_6_retention AS (
  SELECT
    subscription_start_week,
    COUNT(DISTINCT user_pseudo_id) AS week_6_retained_users
  FROM cohort_data
  WHERE DATE_ADD(subscription_start_week, INTERVAL 6 WEEK) < subscription_end_week OR subscription_end_week IS NULL
  GROUP BY 1
)
SELECT 
  user_counts.subscription_start_week,
  total_users,
  week_0_retained_users,
  week_1_retained_users,
  week_2_retained_users,
  week_3_retained_users,
  week_4_retained_users,
  week_5_retained_users,
  week_6_retained_users
FROM user_counts
JOIN week_0_retention ON user_counts.subscription_start_week = week_0_retention.subscription_start_week
JOIN week_1_retention ON user_counts.subscription_start_week = week_1_retention.subscription_start_week
JOIN week_2_retention ON user_counts.subscription_start_week = week_2_retention.subscription_start_week
JOIN week_3_retention ON user_counts.subscription_start_week = week_3_retention.subscription_start_week
JOIN week_4_retention ON user_counts.subscription_start_week = week_4_retention.subscription_start_week
JOIN week_5_retention ON user_counts.subscription_start_week = week_5_retention.subscription_start_week
JOIN week_6_retention ON user_counts.subscription_start_week = week_6_retention.subscription_start_week
ORDER BY subscription_start_week;
