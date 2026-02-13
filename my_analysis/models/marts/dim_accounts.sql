{{ config(materialized='table') }}

WITH raw_accounts AS (
    SELECT * FROM {{ source('saas_raw', 'accounts') }}
),

final_cleaning AS (
    SELECT
        account_id,
        UPPER(account_name) AS account_name,
        industry,
        country,
        REFERRAL_SOURCE,
        TO_DATE(signup_date) AS signup_date,
        CASE 
            WHEN seats > 50 THEN 'Enterprise'
            WHEN seats BETWEEN 11 AND 50 THEN 'Mid-Market'
            ELSE 'SMB'
        END AS account_segment,
        DATEDIFF('month', TO_DATE(signup_date), CURRENT_DATE()) AS tenure_months,
        plan_tier,
        seats,
        is_trial,
        churn_flag
    FROM raw_accounts
)

SELECT * FROM final_cleaning