-- Financial Fact Table: Subscriptions (Keep comments in English)
{{ config(materialized='table') }}

WITH dim_accounts AS (
    SELECT * FROM {{ ref('dim_accounts') }}
),

raw_sub AS (
    SELECT * FROM {{ source('saas_raw', 'subscriptions') }}
),

final_subscriptions AS (
    SELECT
        s.SUBSCRIPTION_ID,
        s.ACCOUNT_ID,
        -- Bringing metadata from dim_accounts
        a.ACCOUNT_NAME,
        a.ACCOUNT_SEGMENT,
        a.COUNTRY,
        
        -- Date conversions
        TO_DATE(s.START_DATE) AS START_DATE,
        TO_DATE(s.END_DATE) AS END_DATE,
        
        -- Financial metrics
        s.PLAN_TIER,
        s.MRR_AMOUNT, -- Monthly Recurring Revenue
        s.ARR_AMOUNT, -- Annual Recurring Revenue
        s.BILLING_FREQUENCY,
        
        -- Flags
        s.IS_TRIAL,
        s.UPGRADE_FLAG,
        s.DOWNGRADE_FLAG,
        s.CHURN_FLAG,
        
        -- Calculating Subscription Duration in days
        DATEDIFF('day', TO_DATE(s.START_DATE), TO_DATE(s.END_DATE)) AS SUBSCRIPTION_LIFESPAN_DAYS

    FROM raw_sub s
    LEFT JOIN dim_accounts a ON s.ACCOUNT_ID = a.ACCOUNT_ID
)

SELECT * FROM final_subscriptions