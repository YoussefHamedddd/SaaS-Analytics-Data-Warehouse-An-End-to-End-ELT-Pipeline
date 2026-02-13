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
    with open('queries.sql', 'r') as f:
        sql_file = f.read()

    commands = sql_file.split(';')

    for command in commands:
        if command.strip(): # 
            print(f"Executing: {command[:50]}...") 
            cur.execute(command)
            
    print("--- All commands executed successfully! ---")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    cur.close()
    conn.close()