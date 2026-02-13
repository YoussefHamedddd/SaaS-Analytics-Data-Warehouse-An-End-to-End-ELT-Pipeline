-- Fact table for Support Tickets (Keep comments in English)
{{ config(materialized='table') }}

WITH dim_accounts AS (
    SELECT * FROM {{ ref('dim_accounts') }}
),

raw_tickets AS (
    SELECT * FROM {{ source('saas_raw', 'support_tickets') }}
),

final_tickets AS (
    SELECT
        t.TICKET_ID,
        t.ACCOUNT_ID,
        a.ACCOUNT_NAME,
        a.ACCOUNT_SEGMENT,
        
        -- Handling timestamps
        TO_TIMESTAMP(t.SUBMITTED_AT) AS SUBMITTED_AT,
        TO_TIMESTAMP(t.CLOSED_AT) AS CLOSED_AT,
        
        t.PRIORITY,
        t.RESOLUTION_TIME_HOURS,
        t.SATISFACTION_SCORE,
        t.ESCALATION_FLAG,
        
        -- Custom Logic: Ticket Status
        CASE 
            WHEN t.CLOSED_AT IS NULL THEN 'Open'
            ELSE 'Resolved'
        END AS TICKET_STATUS

    FROM raw_tickets t
    LEFT JOIN dim_accounts a ON t.ACCOUNT_ID = a.ACCOUNT_ID
)

SELECT * FROM final_tickets