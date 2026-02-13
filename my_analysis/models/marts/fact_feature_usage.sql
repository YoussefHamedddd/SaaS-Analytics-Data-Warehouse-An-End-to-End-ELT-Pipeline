-- Fact table for Product Usage (Keep comments in English)
{{ config(materialized='table') }}

WITH raw_usage AS (
    SELECT * FROM {{ source('saas_raw', 'feature_usage') }}
),

-- Link with subscriptions to understand context
subscriptions AS (
    SELECT * FROM {{ ref('fact_subscriptions') }}
),

final_usage AS (
    SELECT
        u.USAGE_ID,
        u.SUBSCRIPTION_ID,
        s.ACCOUNT_ID,
        s.ACCOUNT_NAME,
        s.ACCOUNT_SEGMENT,
        
        TO_DATE(u.USAGE_DATE) AS USAGE_DATE,
        u.FEATURE_NAME,
        u.USAGE_COUNT,
        u.USAGE_DURATION_SECS,
        u.ERROR_COUNT,
        u.IS_BETA_FEATURE,
        
        -- Efficiency Metric: Usage per Error
        CASE 
            WHEN u.ERROR_COUNT = 0 THEN u.USAGE_COUNT 
            ELSE (u.USAGE_COUNT / u.ERROR_COUNT) 
        END AS USAGE_EFFICIENCY_RATIO

    FROM raw_usage u
    LEFT JOIN subscriptions s ON u.SUBSCRIPTION_ID = s.SUBSCRIPTION_ID
)

SELECT * FROM final_usage