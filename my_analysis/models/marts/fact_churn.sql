-- Fact table for Churn Analysis (Keep comments in English)
{{ config(materialized='table') }}

WITH dim_accounts AS (
    -- Referencing the table we already cleaned
    SELECT * FROM {{ ref('dim_accounts') }}
),

raw_churn AS (
    -- Getting the raw churn events
    SELECT * FROM {{ source('saas_raw', 'churn_events') }}
),

final_churn_analysis AS (
    SELECT
        c.CHURN_EVENT_ID,
        c.ACCOUNT_ID,
        -- Reference data from dim_accounts to know WHO churned
        a.ACCOUNT_NAME,
        a.ACCOUNT_SEGMENT,
        a.COUNTRY,
        a.PLAN_TIER,
        a.SEATS,
        
        -- Clean the date
        TO_DATE(c.CHURN_DATE) AS CHURN_DATE,
        
        c.REASON_CODE,
        -- Handle Nulls in refund (If no refund, set to 0)
        COALESCE(c.REFUND_AMOUNT_USD, 0) AS REFUND_AMOUNT_USD,
        
        c.PRECEDING_UPGRADE_FLAG,
        c.PRECEDING_DOWNGRADE_FLAG,
        c.IS_REACTIVATION,
        c.FEEDBACK_TEXT,
        
        -- Calculate financial impact (Simplified example: seats * some price factor)
        -- Let's say each seat is worth $50
        (a.SEATS * 50) AS ESTIMATED_LOST_MRR

    FROM raw_churn c
    LEFT JOIN dim_accounts a ON c.ACCOUNT_ID = a.ACCOUNT_ID
)

SELECT * FROM final_churn_analysis