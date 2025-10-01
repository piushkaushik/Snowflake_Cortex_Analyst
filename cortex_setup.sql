import snowflake.connector

# Create connection
conn = snowflake.connector.connect(
    user='PIYUSH',
    password='#No@9761249627',
    account='GOHXHBW-XMA47905',
    warehouse='CORTEX_ANALYST_WH',
    database='OPTIMIZER',
    schema='DATA'
)

# Create cursor and execute command
cursor = conn.cursor()
cursor.execute("PUT file://semantic_model_updated.yaml @semantic_models_stage/")

# Close cursor and connection
cursor.close()
conn.close()