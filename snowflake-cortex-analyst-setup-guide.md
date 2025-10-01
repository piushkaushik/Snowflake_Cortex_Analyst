# Snowflake Cortex Analyst - Complete Setup Guide

**Project: Natural Language Analytics with Snowflake Cortex Analyst**

This comprehensive guide walks you through implementing Snowflake Cortex Analyst from scratch using a new trial account in East US 2 (Virginia) region.

## 🚀 Overview

This project demonstrates how to:
- Set up Snowflake Cortex Analyst for natural language queries
- Create semantic models for ecommerce analytics data
- Build Python applications to interact with Cortex Analyst
- Enable business users to query data using plain English

---

## 📋 Prerequisites

### 1. System Requirements
- **Operating System**: Windows 10+, macOS 10.14+, or Linux
- **Python**: Version 3.8 or higher
- **Internet Connection**: Stable connection for Snowflake access
- **Storage**: At least 500 MB free space

### 2. Software Installation
```bash
# Install Python packages
pip install snowflake-connector-python==3.12.4
pip install requests==2.31.0
pip install pyyaml==6.0.1
pip install streamlit==1.28.0
```

### 3. Optional: SnowSQL CLI
Download from: https://docs.snowflake.com/en/user-guide/snowsql-install-config

---

## 🏗️ Step-by-Step Implementation

### STEP 1: Create Snowflake Trial Account

1. **Visit Snowflake Signup Page**
   - Go to: https://signup.snowflake.com/
   - Click "Start for free"

2. **Account Configuration**
   - **Cloud Provider**: Select **Amazon Web Services (AWS)**
   - **Region**: Select **East US 2 (Virginia)** - `AWS us-east-1`
   - **Edition**: Choose **Enterprise** (30-day free trial)

3. **Account Details**
   ```
   First Name: [Your Name]
   Last Name: [Your Last Name]
   Email: [Your Email]
   Company: [Your Company]
   Password: [Secure Password]
   ```

4. **Activate Account**
   - Check your email for activation link
   - Click activation link and set up account
   - Note your **Account Identifier** (e.g., `XXXXXXX-XXXXXXX`)

### STEP 2: Initial Snowflake Setup

1. **Login to Snowsight**
   - URL: `https://app.snowflake.com/`
   - Use your credentials to login

2. **Verify Account Region**
   ```sql
   SELECT CURRENT_REGION();
   ```
   Expected result: `AWS_US_EAST_1`

3. **Check Account Edition**
   ```sql
   SELECT CURRENT_ACCOUNT_NAME(), CURRENT_ACCOUNT();
   ```

### STEP 3: Enable Cortex Analyst

1. **Run as ACCOUNTADMIN**
   ```sql
   USE ROLE ACCOUNTADMIN;

   -- Enable Cortex Analyst
   ALTER ACCOUNT SET ENABLE_CORTEX_ANALYST = TRUE;

   -- Allow all Cortex models
   ALTER ACCOUNT SET CORTEX_MODELS_ALLOWLIST = 'ALL';
   
   -- Verify settings
   SHOW PARAMETERS LIKE 'ENABLE_CORTEX_ANALYST' IN ACCOUNT;
   SHOW PARAMETERS LIKE 'CORTEX_MODELS_ALLOWLIST' IN ACCOUNT;
   ```

### STEP 4: Create Database and Load Sample Data

1. **Create Database Structure**
   ```sql
   USE ROLE SYSADMIN;

   -- Create database and schema
   CREATE DATABASE OPTIMIZER;
   CREATE SCHEMA OPTIMIZER.DATA;

   -- Use the schema
   USE DATABASE OPTIMIZER;
   USE SCHEMA DATA;
   ```

2. **Create Sample Tables**
   ```sql
   -- Create WEB_DATA table
   CREATE OR REPLACE TABLE WEB_DATA (
       "Revenue" FLOAT,
       "Units" FLOAT,
       "Product_Name" VARCHAR(100),
       "Device_Type" VARCHAR(50),
       "Marketing_Channel" VARCHAR(50),
       "Payment_Method" VARCHAR(50),
       "Date" TIMESTAMP_NTZ,
       "Visits" FLOAT,
       "Unique_Visitors" FLOAT,
       "Order" FLOAT,
       "Checkouts" FLOAT,
       "Product_Views" FLOAT,
       "QV%" FLOAT,
       "MV" FLOAT,
       "MV%" FLOAT,
       "ATC" FLOAT,
       "ATC%" FLOAT,
       "Cart_Add" FLOAT,
       "Cart_Abandon" FLOAT,
       "Site_Search" FLOAT,
       "SE%" FLOAT,
       "AOV" FLOAT,
       "Time_Spent_per_Visit" FLOAT
   );

   -- Create APP_DATA table
   CREATE OR REPLACE TABLE APP_DATA (
       "Revenue" FLOAT,
       "Units" FLOAT,
       "Product_Name" VARCHAR(100),
       "Device_Type" VARCHAR(50),
       "Operating_System" VARCHAR(50),
       "Marketing_Channel" VARCHAR(50),
       "Payment_Method" VARCHAR(50),
       "Date" TIMESTAMP_NTZ,
       "Users" FLOAT,
       "Launches" FLOAT,
       "Installs" FLOAT,
       "Orders" FLOAT,
       "Visits" FLOAT,
       "Checkouts" FLOAT,
       "Upgrades" FLOAT,
       "Cart_Abandon" FLOAT,
       "AOV" FLOAT,
       "SE%" FLOAT,
       "QV%" FLOAT,
       "MV%" FLOAT,
       "ATC%" FLOAT,
       "CVR%" FLOAT,
       "Crashes" FLOAT,
       "Time_Spent_per_Visit" FLOAT,
       "Product_Views" FLOAT,
       "Site_Search" FLOAT,
       "Unique_Visitors" FLOAT,
       "Source_Table" VARCHAR(20)
   );
   ```

3. **Insert Sample Data**
   ```sql
   -- Insert sample WEB_DATA
   INSERT INTO WEB_DATA VALUES
   (1500.0, 25, 'iPhone 15', 'Mobile', 'Google Ads', 'Credit Card', '2024-01-01'::timestamp, 100, 80, 5, 8, 50, 5.0, 10, 10.0, 3, 3.0, 5, 2, 15, 15.0, 300.0, 4.5),
   (2800.0, 40, 'MacBook Pro', 'Desktop', 'Facebook Ads', 'PayPal', '2024-01-02'::timestamp, 150, 120, 8, 12, 75, 8.0, 20, 13.3, 6, 4.0, 8, 4, 25, 16.7, 350.0, 6.2),
   (950.0, 15, 'AirPods', 'Mobile', 'Organic Search', 'Apple Pay', '2024-01-03'::timestamp, 80, 65, 3, 5, 40, 3.8, 8, 10.0, 2, 2.5, 4, 1, 12, 15.0, 63.3, 3.1);

   -- Insert sample APP_DATA
   INSERT INTO APP_DATA VALUES
   (1200.0, 20, 'iPhone 15', 'Mobile', 'iOS', 'App Store', 'Apple Pay', '2024-01-01'::timestamp, 200, 500, 50, 4, 80, 6, 5, 1, 300.0, 12.0, 5.0, 8.0, 2.5, 5.0, 2, 5.2, 35, 18, 160, 'APP'),
   (1800.0, 30, 'MacBook Pro', 'Tablet', 'iOS', 'Social Media', 'Credit Card', '2024-01-02'::timestamp, 180, 450, 45, 6, 70, 8, 4, 2, 300.0, 13.3, 8.6, 11.4, 3.6, 8.6, 1, 6.1, 30, 20, 140, 'APP'),
   (800.0, 12, 'AirPods', 'Mobile', 'Android', 'Google Play', 'PayPal', '2024-01-03'::timestamp, 150, 380, 38, 3, 60, 5, 3, 1, 266.7, 11.8, 5.0, 8.0, 2.0, 5.0, 0, 4.8, 25, 15, 120, 'APP');

   -- Verify data
   SELECT COUNT(*) FROM WEB_DATA;
   SELECT COUNT(*) FROM APP_DATA;
   ```

### STEP 5: Create Cortex Analyst Roles and Permissions

1. **Create Custom Role**
   ```sql
   USE ROLE SECURITYADMIN;

   -- Create Cortex Analyst role
   CREATE ROLE IF NOT EXISTS cortex_analyst_role;

   -- Grant Cortex User database role
   GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE cortex_analyst_role;

   -- Grant your user the new role
   GRANT ROLE cortex_analyst_role TO USER [YOUR_USERNAME];

   -- Switch to SYSADMIN for object privileges
   USE ROLE SYSADMIN;

   -- Grant database and schema usage
   GRANT USAGE ON DATABASE OPTIMIZER TO ROLE cortex_analyst_role;
   GRANT USAGE ON SCHEMA OPTIMIZER.DATA TO ROLE cortex_analyst_role;

   -- Grant table access
   GRANT SELECT ON TABLE OPTIMIZER.DATA.WEB_DATA TO ROLE cortex_analyst_role;
   GRANT SELECT ON TABLE OPTIMIZER.DATA.APP_DATA TO ROLE cortex_analyst_role;
   ```

2. **Create Warehouse**
   ```sql
   USE ROLE SYSADMIN;

   CREATE OR REPLACE WAREHOUSE CORTEX_ANALYST_WH
   WITH 
       WAREHOUSE_SIZE = 'SMALL'
       AUTO_SUSPEND = 60
       AUTO_RESUME = TRUE
       INITIALLY_SUSPENDED = TRUE
       COMMENT = 'Warehouse for Cortex Analyst operations';

   -- Grant warehouse privileges
   GRANT USAGE ON WAREHOUSE CORTEX_ANALYST_WH TO ROLE cortex_analyst_role;
   GRANT OPERATE ON WAREHOUSE CORTEX_ANALYST_WH TO ROLE cortex_analyst_role;
   ```

### STEP 6: Create Stage for Semantic Models

```sql
USE ROLE SYSADMIN;
USE DATABASE OPTIMIZER;
USE SCHEMA DATA;

-- Create stage for semantic models
CREATE OR REPLACE STAGE semantic_models_stage
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for storing Cortex Analyst semantic models';

-- Grant stage privileges
GRANT READ ON STAGE semantic_models_stage TO ROLE cortex_analyst_role;
GRANT WRITE ON STAGE semantic_models_stage TO ROLE cortex_analyst_role;

-- Verify stage creation
SHOW STAGES LIKE 'semantic_models_stage';
```

### STEP 7: Create Semantic Model YAML

1. **Create semantic_model.yaml file** on your local machine:

```yaml
name: 'ecommerce_analytics_model'
description: 'Semantic model for e-commerce web and app analytics data'
tables:
  - name: 'web_analytics'
    description: 'Web analytics data including revenue, visits, and user behavior metrics'
    base_table:
      database: 'OPTIMIZER'
      schema: 'DATA'
      table: 'WEB_DATA'
    dimensions:
      - name: 'product_name'
        synonyms: ['product', 'item', 'product name']
        description: 'Name of the product'
        expr: '"Product_Name"'
        data_type: 'TEXT'
      - name: 'device_type'
        synonyms: ['device', 'platform', 'device type']
        description: 'Type of device used'
        expr: '"Device_Type"'
        data_type: 'TEXT'
      - name: 'marketing_channel'
        synonyms: ['channel', 'marketing channel', 'traffic source']
        description: 'Marketing channel or traffic source'
        expr: '"Marketing_Channel"'
        data_type: 'TEXT'
      - name: 'payment_method'
        synonyms: ['payment', 'payment type']
        description: 'Payment method used'
        expr: '"Payment_Method"'
        data_type: 'TEXT'
    time_dimensions:
      - name: 'date'
        synonyms: ['day', 'transaction date', 'order date']
        description: 'Date of the transaction or event'
        expr: '"Date"'
        data_type: 'TIMESTAMP_NTZ'
    facts:
      - name: 'revenue'
        synonyms: ['sales', 'total revenue', 'income']
        description: 'Total revenue generated'
        expr: '"Revenue"'
        data_type: 'FLOAT'
      - name: 'units'
        synonyms: ['quantity', 'units sold', 'products sold']
        description: 'Number of units sold'
        expr: '"Units"'
        data_type: 'FLOAT'
      - name: 'visits'
        synonyms: ['sessions', 'website visits']
        description: 'Number of visits to the website'
        expr: '"Visits"'
        data_type: 'FLOAT'
      - name: 'unique_visitors'
        synonyms: ['unique users', 'distinct visitors']
        description: 'Number of unique visitors'
        expr: '"Unique_Visitors"'
        data_type: 'FLOAT'
      - name: 'orders'
        synonyms: ['transactions', 'purchases']
        description: 'Number of completed orders'
        expr: '"Order"'
        data_type: 'FLOAT'
    metrics:
      - name: 'conversion_rate'
        synonyms: ['CR', 'order rate']
        description: 'Conversion rate as orders/visits'
        expr: 'SUM("Order") / SUM("Visits")'
        data_type: 'FLOAT'
      - name: 'average_order_value'
        synonyms: ['AOV', 'avg order value']
        description: 'Average order value'
        expr: 'SUM("Revenue") / SUM("Order")'
        data_type: 'FLOAT'

  - name: 'app_analytics'
    description: 'Mobile app analytics data'
    base_table:
      database: 'OPTIMIZER'
      schema: 'DATA'
      table: 'APP_DATA'
    dimensions:
      - name: 'product_name_app'
        synonyms: ['product', 'item', 'app product']
        description: 'Product name in app'
        expr: '"Product_Name"'
        data_type: 'TEXT'
      - name: 'operating_system'
        synonyms: ['OS', 'platform', 'mobile OS']
        description: 'Operating system of the device'
        expr: '"Operating_System"'
        data_type: 'TEXT'
      - name: 'device_type_app'
        synonyms: ['device', 'mobile device']
        description: 'Type of mobile device'
        expr: '"Device_Type"'
        data_type: 'TEXT'
    time_dimensions:
      - name: 'date_app'
        synonyms: ['app date', 'transaction date']
        description: 'Date of app event'
        expr: '"Date"'
        data_type: 'TIMESTAMP_NTZ'
    facts:
      - name: 'revenue_app'
        synonyms: ['app revenue', 'mobile revenue']
        description: 'Revenue from app'
        expr: '"Revenue"'
        data_type: 'FLOAT'
      - name: 'users'
        synonyms: ['app users', 'mobile users']
        description: 'Number of app users'
        expr: '"Users"'
        data_type: 'FLOAT'
      - name: 'launches'
        synonyms: ['app launches', 'app opens']
        description: 'Number of app launches'
        expr: '"Launches"'
        data_type: 'FLOAT'
      - name: 'installs'
        synonyms: ['app installs', 'downloads']
        description: 'Number of app installations'
        expr: '"Installs"'
        data_type: 'FLOAT'

verified_queries:
  - name: 'total_revenue_by_channel'
    question: 'What is the total revenue by marketing channel?'
    sql: 'SELECT "Marketing_Channel", SUM("Revenue") as total_revenue FROM WEB_DATA GROUP BY "Marketing_Channel" ORDER BY total_revenue DESC'
    use_as_onboarding_question: true
  - name: 'app_installs_count'
    question: 'How many app installs do we have?'
    sql: 'SELECT SUM("Installs") as total_installs FROM APP_DATA'
    use_as_onboarding_question: true
```

### STEP 8: Upload Semantic Model

**Option A: Using SnowSQL (Recommended)**
```bash
# Connect to Snowflake
snowsql -a [YOUR_ACCOUNT] -u [YOUR_USERNAME] -w CORTEX_ANALYST_WH -d OPTIMIZER -s DATA

# Upload semantic model
PUT file://semantic_model.yaml @semantic_models_stage/;
```

**Option B: Using Python Script**
```python
import snowflake.connector

conn = snowflake.connector.connect(
    user='[YOUR_USERNAME]',
    password='[YOUR_PASSWORD]',
    account='[YOUR_ACCOUNT]',
    warehouse='CORTEX_ANALYST_WH',
    database='OPTIMIZER',
    schema='DATA'
)

cursor = conn.cursor()
cursor.execute("PUT file://semantic_model.yaml @semantic_models_stage/")
cursor.close()
conn.close()
```

**Verify Upload:**
```sql
LIST @semantic_models_stage;
```

### STEP 9: Test Cortex Analyst

1. **Basic Test Query**
   ```sql
   USE ROLE cortex_analyst_role;
   USE WAREHOUSE CORTEX_ANALYST_WH;
   USE DATABASE OPTIMIZER;
   USE SCHEMA DATA;

   -- Test Cortex Analyst
   SELECT SNOWFLAKE.CORTEX.ANALYST(
       'What is the total revenue by marketing channel?',
       '@semantic_models_stage/semantic_model.yaml'
   ) as result;
   ```

2. **Additional Test Queries**
   ```sql
   -- Test 2: Device type analysis
   SELECT SNOWFLAKE.CORTEX.ANALYST(
       'Show me revenue by device type',
       '@semantic_models_stage/semantic_model.yaml'
   ) as result;

   -- Test 3: App performance
   SELECT SNOWFLAKE.CORTEX.ANALYST(
       'How many app installs do we have?',
       '@semantic_models_stage/semantic_model.yaml'
   ) as result;
   ```

### STEP 10: Create Python Application

1. **Create cortex_analyst_app.py**:

```python
import snowflake.connector
import requests
import json
import streamlit as st
from typing import Dict, Any

class CortexAnalystApp:
    def __init__(self, account: str, user: str, password: str):
        self.account = account
        self.user = user
        self.password = password
        self.connection = None
        self.session_token = None
        
    def connect(self):
        """Connect to Snowflake"""
        try:
            self.connection = snowflake.connector.connect(
                user=self.user,
                password=self.password,
                account=self.account,
                warehouse='CORTEX_ANALYST_WH',
                database='OPTIMIZER',
                schema='DATA'
            )
            self.session_token = self.connection.rest.token
            return True
        except Exception as e:
            st.error(f"Connection failed: {e}")
            return False
    
    def query_analyst(self, question: str) -> Dict[str, Any]:
        """Query Cortex Analyst"""
        try:
            request_body = {
                "messages": [{
                    "role": "user",
                    "content": [{
                        "type": "text",
                        "text": question
                    }]
                }],
                "semantic_model_file": "@semantic_models_stage/semantic_model.yaml"
            }
            
            url = f"https://{self.account}.snowflakecomputing.com/api/v2/cortex/analyst/message"
            headers = {
                "Authorization": f'Snowflake Token="{self.session_token}"',
                "Content-Type": "application/json"
            }
            
            response = requests.post(url, json=request_body, headers=headers)
            
            if response.status_code == 200:
                return response.json()
            else:
                return {"error": f"Status {response.status_code}: {response.text}"}
                
        except Exception as e:
            return {"error": str(e)}

# Streamlit UI
def main():
    st.title("🤖 Snowflake Cortex Analyst Demo")
    st.write("Ask questions about your ecommerce data in natural language!")
    
    # Sidebar for configuration
    with st.sidebar:
        st.header("Configuration")
        account = st.text_input("Snowflake Account", placeholder="XXXXXXX-XXXXXXX")
        user = st.text_input("Username")
        password = st.text_input("Password", type="password")
        
        connect_btn = st.button("Connect to Snowflake")
    
    if connect_btn and account and user and password:
        app = CortexAnalystApp(account, user, password)
        if app.connect():
            st.session_state['app'] = app
            st.success("✅ Connected to Snowflake!")
    
    # Main chat interface
    if 'app' in st.session_state:
        app = st.session_state['app']
        
        # Sample questions
        st.subheader("📝 Try these sample questions:")
        sample_questions = [
            "What is the total revenue by marketing channel?",
            "Show me conversion rates by device type",
            "How many app installs do we have?",
            "What is the average order value?",
            "Compare web vs app revenue performance"
        ]
        
        for question in sample_questions:
            if st.button(question, key=f"sample_{question}"):
                with st.spinner("🤔 Thinking..."):
                    result = app.query_analyst(question)
                    
                    if "error" not in result:
                        st.success("✅ Query successful!")
                        
                        # Display response
                        if "message" in result and "content" in result["message"]:
                            for content in result["message"]["content"]:
                                if content["type"] == "text":
                                    st.write("**Answer:**", content["text"])
                                elif content["type"] == "sql":
                                    st.code(content["statement"], language="sql")
                    else:
                        st.error(f"❌ Error: {result['error']}")
        
        # Custom question input
        st.subheader("💬 Ask your own question:")
        custom_question = st.text_input("Enter your question:")
        
        if st.button("Ask Question") and custom_question:
            with st.spinner("🤔 Analyzing your question..."):
                result = app.query_analyst(custom_question)
                
                if "error" not in result:
                    st.success("✅ Query successful!")
                    
                    if "message" in result and "content" in result["message"]:
                        for content in result["message"]["content"]:
                            if content["type"] == "text":
                                st.write("**Answer:**", content["text"])
                            elif content["type"] == "sql":
                                st.code(content["statement"], language="sql")
                else:
                    st.error(f"❌ Error: {result['error']}")

if __name__ == "__main__":
    main()
```

2. **Run the Streamlit App**:
   ```bash
   streamlit run cortex_analyst_app.py
   ```

### STEP 11: Testing and Validation

1. **Test Sample Questions**:
   - "What is the total revenue by marketing channel?"
   - "Show me conversion rates by device type"
   - "How many app installs do we have?"
   - "What is the average order value?"
   - "Compare web vs app performance"

2. **Verify Results**:
   - Check that SQL is generated correctly
   - Confirm data results match expectations
   - Test edge cases and complex questions

---

## 🎯 Expected Results

After completing this setup, you should be able to:

1. ✅ **Natural Language Queries**: Ask business questions in plain English
2. ✅ **Automatic SQL Generation**: Cortex Analyst generates SQL automatically
3. ✅ **Real-time Results**: Get instant answers to your data questions
4. ✅ **Multi-table Analytics**: Query across web and app data seamlessly
5. ✅ **Business Insights**: Derive actionable insights from your data

---

## 🔧 Troubleshooting

### Common Issues and Solutions

1. **Connection Error**
   - Verify account identifier is correct
   - Check username/password
   - Ensure region is East US 2

2. **Permission Denied**
   - Confirm all role grants are applied
   - Switch to correct role before querying
   - Verify warehouse permissions

3. **Semantic Model Issues**
   - Check YAML syntax is valid
   - Ensure column names match exactly (with quotes)
   - Verify file upload to stage

4. **API Errors**
   - Confirm Cortex Analyst is enabled
   - Check model allowlist settings
   - Verify session token is valid

---

## 📚 Additional Resources

- **Snowflake Cortex Documentation**: https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst
- **Semantic Model Reference**: https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst/semantic-model-spec
- **API Documentation**: https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst/rest-api

---

## 🎉 Conclusion

You now have a fully functional Snowflake Cortex Analyst implementation that allows natural language queries against your ecommerce analytics data. This setup provides a foundation for building more sophisticated AI-powered analytics applications.

The system enables business users to get insights without writing SQL, democratizing data access across your organization.