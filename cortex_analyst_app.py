
"""
Snowflake Cortex Analyst Streamlit Application
==============================================

A complete Streamlit application that provides a chat interface
for interacting with Snowflake Cortex Analyst.

Installation:
pip install streamlit snowflake-connector-python requests pyyaml

Usage:
streamlit run cortex_analyst_app.py
"""

import streamlit as st
import snowflake.connector
import requests
import json
import yaml
from typing import Dict, List, Any, Optional
import time
import pandas as pd

# Page configuration
st.set_page_config(
    page_title="Cortex Analyst Chat",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better UI
st.markdown("""
<style>
    .chat-message {
        padding: 1rem;
        border-radius: 0.5rem;
        margin-bottom: 1rem;
        display: flex;
        flex-direction: column;
    }
    .user-message {
        background-color: #e3f2fd;
        border-left: 4px solid #2196f3;
    }
    .assistant-message {
        background-color: #f3e5f5;
        border-left: 4px solid #9c27b0;
    }
    .sql-code {
        background-color: #f5f5f5;
        border: 1px solid #ddd;
        border-radius: 4px;
        padding: 1rem;
        font-family: 'Courier New', monospace;
        white-space: pre-wrap;
        margin: 0.5rem 0;
    }
    .metric-card {
        background-color: #ffffff;
        padding: 1rem;
        border-radius: 0.5rem;
        border: 1px solid #e0e0e0;
        margin: 0.5rem 0;
    }
</style>
""", unsafe_allow_html=True)

class CortexAnalystChat:
    """Class to handle Cortex Analyst interactions"""

    def __init__(self):
        self.connection = None
        self.session_token = None

    def connect_to_snowflake(self, account: str, user: str, password: str, 
                           warehouse: str, database: str, schema: str) -> bool:
        """Connect to Snowflake and get session token"""
        try:
            self.connection = snowflake.connector.connect(
                user=user,
                password=password,
                account=account,
                warehouse=warehouse,
                database=database,
                schema=schema
            )
            self.session_token = self.connection.rest.token
            return True
        except Exception as e:
            st.error(f"Connection failed: {str(e)}")
            return False

    def send_message(self, prompt: str, semantic_model_file: str) -> Dict[str, Any]:
        """Send message to Cortex Analyst API"""
        try:
            request_body = {
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": prompt
                            }
                        ]
                    }
                ],
                "semantic_model_file": semantic_model_file
            }

            # Extract account from connection
            account = self.connection.host.split('.')[0]
            url = f"https://{self.connection.host}/api/v2/cortex/analyst/message"

            headers = {
                "Authorization": f'Snowflake Token="{self.session_token}"',
                "Content-Type": "application/json"
            }

            response = requests.post(url, json=request_body, headers=headers)

            if response.status_code == 200:
                return response.json()
            else:
                return {
                    "error": f"API Error {response.status_code}: {response.text}"
                }

        except Exception as e:
            return {"error": f"Request failed: {str(e)}"}

    def execute_sql(self, sql_query: str) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql_query)

            # Get column names
            columns = [desc[0] for desc in cursor.description]

            # Get data
            data = cursor.fetchall()
            cursor.close()

            # Create DataFrame
            df = pd.DataFrame(data, columns=columns)
            return df

        except Exception as e:
            st.error(f"SQL execution failed: {str(e)}")
            return pd.DataFrame()

def initialize_session_state():
    """Initialize Streamlit session state"""
    if 'chat_history' not in st.session_state:
        st.session_state.chat_history = []
    if 'cortex_analyst' not in st.session_state:
        st.session_state.cortex_analyst = CortexAnalystChat()
    if 'connected' not in st.session_state:
        st.session_state.connected = False

def display_chat_message(message_type: str, content: str, sql_query: str = None, 
                        df: pd.DataFrame = None):
    """Display a chat message with proper formatting"""

    message_class = "user-message" if message_type == "user" else "assistant-message"
    icon = "🙋‍♂️" if message_type == "user" else "🧠"

    with st.container():
        st.markdown(f"""
        <div class="chat-message {message_class}">
            <strong>{icon} {"You" if message_type == "user" else "Cortex Analyst"}</strong>
            <div style="margin-top: 0.5rem;">{content}</div>
        </div>
        """, unsafe_allow_html=True)

        # Display SQL query if provided
        if sql_query:
            with st.expander("View Generated SQL"):
                st.code(sql_query, language="sql")

        # Display DataFrame if provided
        if df is not None and not df.empty:
            st.dataframe(df, use_container_width=True)

def main():
    """Main Streamlit application"""

    initialize_session_state()

    # Title and description
    st.title("🧠 Snowflake Cortex Analyst Chat")
    st.markdown("Ask questions about your data in natural language!")

    # Sidebar for configuration
    with st.sidebar:
        st.header("🔧 Configuration")

        # Connection settings
        st.subheader("Snowflake Connection")

        account = st.text_input("Account Identifier", 
                               placeholder="abc12345.us-east-1",
                               help="Your Snowflake account identifier")
        user = st.text_input("Username", placeholder="your_username")
        password = st.text_input("Password", type="password")
        warehouse = st.text_input("Warehouse", value="CORTEX_ANALYST_WH")
        database = st.text_input("Database", value="OPTIMIZER")
        schema = st.text_input("Schema", value="DATA")

        # Semantic model configuration
        st.subheader("Semantic Model")
        semantic_model_file = st.text_input(
            "Semantic Model File",
            value="@OPTIMIZER.DATA.SEMANTIC_MODELS_STAGE/semantic_model.yaml",
            help="Path to your semantic model YAML file in Snowflake stage"
        )

        # Connect button
        if st.button("🔌 Connect to Snowflake", type="primary"):
            if all([account, user, password, warehouse, database, schema]):
                with st.spinner("Connecting to Snowflake..."):
                    success = st.session_state.cortex_analyst.connect_to_snowflake(
                        account, user, password, warehouse, database, schema
                    )
                    if success:
                        st.session_state.connected = True
                        st.success("✅ Connected to Snowflake!")
                    else:
                        st.session_state.connected = False
            else:
                st.error("Please fill in all connection details")

        # Connection status
        if st.session_state.connected:
            st.success("🟢 Connected")
        else:
            st.error("🔴 Not connected")

        # Sample questions
        st.subheader("💡 Sample Questions")
        sample_questions = [
            "What is the total revenue from web data?",
            "Show me unique visitors by device type",
            "What is the conversion rate by marketing channel?",
            "How many app installs do we have?",
            "Compare web vs app performance",
            "What is the average order value?",
            "Show me revenue trends over time",
            "Which products have the highest revenue?"
        ]

        for i, question in enumerate(sample_questions):
            if st.button(f"📝 {question}", key=f"sample_{i}"):
                if st.session_state.connected:
                    st.session_state.user_input = question
                else:
                    st.warning("Please connect to Snowflake first")

    # Main chat interface
    if st.session_state.connected:

        # Display chat history
        for message in st.session_state.chat_history:
            display_chat_message(
                message["type"], 
                message["content"], 
                message.get("sql"), 
                message.get("dataframe")
            )

        # Chat input
        col1, col2 = st.columns([6, 1])

        with col1:
            user_input = st.text_input(
                "Ask a question about your data:",
                placeholder="e.g., What is the total revenue by marketing channel?",
                key="user_input",
                label_visibility="collapsed"
            )

        with col2:
            send_button = st.button("Send", type="primary", use_container_width=True)

        # Process user input
        if (send_button or user_input) and user_input:
            # Add user message to history
            st.session_state.chat_history.append({
                "type": "user",
                "content": user_input
            })

            # Display user message
            display_chat_message("user", user_input)

            # Send to Cortex Analyst
            with st.spinner("🧠 Cortex Analyst is thinking..."):
                response = st.session_state.cortex_analyst.send_message(
                    user_input, semantic_model_file
                )

            # Process response
            if "error" in response:
                error_msg = f"❌ Error: {response['error']}"
                st.session_state.chat_history.append({
                    "type": "assistant",
                    "content": error_msg
                })
                display_chat_message("assistant", error_msg)

            else:
                # Extract response components
                assistant_text = ""
                sql_query = None
                df = None

                if "message" in response and "content" in response["message"]:
                    for content in response["message"]["content"]:
                        if content["type"] == "text":
                            assistant_text = content["text"]
                        elif content["type"] == "sql":
                            sql_query = content["statement"]

                            # Execute SQL if present
                            if sql_query:
                                df = st.session_state.cortex_analyst.execute_sql(sql_query)

                # Add assistant response to history
                st.session_state.chat_history.append({
                    "type": "assistant",
                    "content": assistant_text or "I analyzed your question and generated a SQL query.",
                    "sql": sql_query,
                    "dataframe": df
                })

                # Display assistant response
                display_chat_message("assistant", 
                                   assistant_text or "I analyzed your question and generated a SQL query.",
                                   sql_query, df)

            # Clear input
            st.session_state.user_input = ""
            st.rerun()

    else:
        # Instructions for setup
        st.info("👆 Please connect to Snowflake using the sidebar to start chatting with your data.")

        st.markdown("### 🚀 Getting Started")
        st.markdown("""
        1. **Fill in your Snowflake connection details** in the sidebar
        2. **Make sure you have:**
           - A Snowflake Business Critical account (or higher)
           - The CORTEX_USER role granted to your user
           - Access to the OPTIMIZER.DATA.WEB_DATA and APP_DATA tables
           - A semantic model YAML file uploaded to a Snowflake stage
        3. **Click "Connect to Snowflake"**
        4. **Start asking questions** about your data in natural language!
        """)

        st.markdown("### 📊 What You Can Ask")
        st.markdown("""
        - **Revenue questions:** "What is the total revenue this month?"
        - **Conversion analysis:** "What is the conversion rate by device type?"
        - **Trend analysis:** "Show me revenue trends over the last 6 months"
        - **Comparisons:** "Compare web vs app performance"
        - **Top performers:** "Which products generate the most revenue?"
        - **User behavior:** "How many unique visitors do we have?"
        """)

    # Footer
    st.markdown("---")
    st.markdown("*Powered by Snowflake Cortex Analyst*")

if __name__ == "__main__":
    main()
