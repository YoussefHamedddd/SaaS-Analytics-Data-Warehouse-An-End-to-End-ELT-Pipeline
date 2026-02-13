import snowflake.connector

conn = snowflake.connector.connect(
    user='',
    password='',
    account='', 
    warehouse='',
    database='',
    schema=''
)


cur = conn.cursor()

try:

    create_table_query = """
    CREATE OR REPLACE TABLE Test (
        ID INT,
        NAME STRING,
        DEPARTMENT STRING,
        HIRE_DATE DATE DEFAULT CURRENT_DATE()
    )
    """
    cur.execute(create_table_query)
    print("table created successfully.")

finally:
    cur.close()
    conn.close()