import os
import json
import re
from dotenv import load_dotenv
from flask import jsonify
from sqlalchemy import create_engine, text
import google.generativeai as genai
from difflib import get_close_matches
import os
from threading import Thread
import pandas as pd
import time
import datetime

export_jobs = {}

class QueryAgent:
    def __init__(self):
        """
        Initializes the QueryAgent by:
        1. Loading environment variables.
        2. Configuring the Google Generative AI (Gemini) client.
        3. Creating a SQLAlchemy engine for the SQL Server DB.
        4. Fetching the database schema to be used in the prompt.
        """
        # ============================================================
        # SECTION 1: Load Environment Variables
        # ============================================================
        load_dotenv()
        
        # ============================================================
        # SECTION 2: Configure Google Generative AI (Gemini)
        # ============================================================
        google_api_key = os.getenv("GOOGLE_API_KEY")
        if not google_api_key:
            raise ValueError("GOOGLE_API_KEY not found in .env file")
        genai.configure(api_key=google_api_key)
        
        # Set model parameters for consistent, safe responses
        generation_config = {
            "temperature": 0.0,  # Deterministic responses
            "top_p": 1,
            "top_k": 1,
            "max_output_tokens": 4096,
        }
        
        # Safety filters to block harmful content
        safety_settings = [
            {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
            {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
            {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
            {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
        ]
        
        # Initialize the Gemini model
        self.model = genai.GenerativeModel(
            model_name="gemini-2.5-flash",
            generation_config=generation_config,
            safety_settings=safety_settings
        )
        
        # ============================================================
        # SECTION 3: Create SQL Server Database Connection
        # ============================================================
        db_server = os.getenv("DB_SERVER")
        db_name = os.getenv("DB_NAME")
        db_user = os.getenv("DB_USERNAME")
        db_pass = os.getenv("DB_PASSWORD")
        db_driver = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")

        
        # Build connection string (with or without credentials)
        if db_user and db_pass:
            conn_string = f"mssql+pyodbc://{db_user}:{db_pass}@{db_server}/{db_name}?driver={db_driver}"
        else:
            conn_string = f"mssql+pyodbc://@{db_server}/{db_name}?driver={db_driver}&Trusted_Connection=yes"

        # Test database connection
        try:
            self.engine = create_engine(conn_string)
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("--- Database Connection Successful ---")
        except Exception as e:
            print(f"--- Database Connection Failed ---")
            print(f"Error: {e}")
            raise
            
        # ============================================================
        # SECTION 4: Load Database Schema for LLM Context
        # ============================================================
        
        # List all the database tables you want the LLM to access.
        # The agent will NOT see any tables not in this list.
        self.relevant_tables = [
            'View_Clean_Imports', 
            'View_Clean_Exports',
        ]
        
        self.schema = self._get_db_schema(self.relevant_tables)
        print(f"--- Schema Loaded ---\n{self.schema}\n-----------------------")

    # ============================================================
    # HELPER METHOD: Fetch Database Schema
    # ============================================================
    def _get_db_schema(self, table_names: list) -> str:
        """Fetches column names and data types for all specified tables."""
        all_schema_parts = []
        try:
            with self.engine.connect() as conn:
                for table_name in table_names:
                    # Query SQL Server information schema for the current table
                    query = text(f"""
                        SELECT COLUMN_NAME, DATA_TYPE 
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_NAME = :table
                    """)
                    result = conn.execute(query, {'table': table_name})
                    rows = result.fetchall()
                    
                    if not rows:
                        print(f"Warning: Table '{table_name}' not found or has no columns.")
                        continue
                        
                    # Format schema as readable string for this table
                    schema_str = f"Table '{table_name}':\n"
                    schema_str += "\n".join([f"- {col[0]} ({col[1]})" for col in rows])
                    all_schema_parts.append(schema_str)
            
            if not all_schema_parts:
                return "Error: No valid tables found in the database."
                
            # Join all individual table schemas with a newline
            return "\n\n".join(all_schema_parts)
            
        except Exception as e:
            print(f"Error fetching schema: {e}")
            return "Error: Could not fetch database schema."

    # ============================================================
    # HELPER METHOD: Clean LLM Response to Extract JSON
    # ============================================================
    def _clean_llm_response(self, response_text):
        """Extracts JSON from LLM response (handles markdown code blocks)"""
        # Try to find JSON wrapped in ```json ... ```
        match = re.search(r'```json\s*(\{.*?\})\s*```', response_text, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1)
        
        # Fallback: Find any JSON object in the response
        match = re.search(r'(\{.*?\})', response_text, re.DOTALL)
        if match:
            return match.group(1)
            
        return None
    
    # ============================================================
    # HELPER METHOD: Fix SQL to Use Normalized Columns (NEW!)
    # ============================================================
    def _fix_product_column_in_sql(self, sql_query: str) -> str:
        """
        Replaces Product_Name/Importer_Name/Exporter_Name with Product/Importer/Exporter
        ONLY in WHERE clauses (for filtering), not in SELECT, GROUP BY, or ORDER BY.
        Also normalizes values to match the UPPERCASE-NO-SPACES format in the database.
        """
        
        # This regex finds the WHERE clause and captures its content,
        # stopping at the next major SQL clause (GROUP BY, ORDER BY, etc.)
        where_clause_regex = r'(\bWHERE\b)(.*?)((?=\bGROUP BY\b|\bORDER BY\b|\bHAVING\b|$))'
        
        def process_where_content(match):
            """
            This inner function is called by re.sub ONLY on the
            content of the WHERE clause.
            """
            where_keyword = match.group(1)  # "WHERE"
            where_content = match.group(2)  # The conditions (e.g., "Product_Name LIKE '%zinc%'")
            terminator = match.group(3)     # The lookahead (e.g., "GROUP BY" or "")
            
            # Step 1: Replace column names ONLY in WHERE clause content
            # where_content = re.sub(r'\bProduct_Name\b', 'Product', where_content, flags=re.IGNORECASE)
            # where_content = re.sub(r'\bImporter_Name\b', 'Importer', where_content, flags=re.IGNORECASE)
            # where_content = re.sub(r'\bExporter_Name\b', 'Exporter', where_content, flags=re.IGNORECASE)
            
            # Step 2: Normalize Product LIKE patterns
            def normalize_product_value(m):
                product_value = m.group(1)
                normalized = product_value.replace(' ', '').upper()
                return f"[Product] LIKE '%{normalized}%'"
            
            where_content = re.sub(
                r"(?:\[?Product\]?|\[?Product_Name\]?)\s+LIKE\s+'%([^']+)%'",
                normalize_product_value,
                where_content,
                flags=re.IGNORECASE
            )
            
            # Step 3: Normalize Importer LIKE patterns
            # def normalize_importer_value(m):
            #     importer_value = m.group(1)
            #     normalized = importer_value.replace(' ', '').upper()
            #     return f"[Importer] LIKE '%{normalized}%'"
            
            # where_content = re.sub(
            #     r"(?:\[?Importer\]?|\[?Importer_Name\]?)\s+LIKE\s+'%([^']+)%'",
            #     normalize_importer_value,
            #     where_content,
            #     flags=re.IGNORECASE
            # )
            
            # Step 4: Normalize Exporter LIKE patterns
            # def normalize_exporter_value(m):
            #     exporter_value = m.group(1)
            #     normalized = exporter_value.replace(' ', '').upper()
            #     return f"[Exporter] LIKE '%{normalized}%'"
            
            # where_content = re.sub(
            #     r"(?:\[?Exporter\]?|\[?Exporter_Name\]?)\s+LIKE\s+'%([^']+)%'",
            #     normalize_exporter_value,
            #     where_content,
            #     flags=re.IGNORECASE
            # )
            
            # Re-assemble the full string
            return where_keyword + where_content + terminator

        # Check if a WHERE clause actually exists. If not, just return the query.
        if not re.search(r'\bWHERE\b', sql_query, flags=re.IGNORECASE):
            return sql_query
            
        # Apply the replacement to the whole query.
        # This will find the WHERE clause and pass it to 'process_where_content'.
        # The SELECT and GROUP BY parts will not be matched or changed.
        fixed_sql = re.sub(
            where_clause_regex, 
            process_where_content, 
            sql_query, 
            count=1,  # Only replace the first (and only) WHERE clause
            flags=re.IGNORECASE | re.DOTALL
        )
        
        return fixed_sql
    
    # ============================================================
    # NEW HELPER METHOD: Generate Summary Statistics Query (Smarter)
    # ============================================================
    def _generate_summary_query(self, original_query: str) -> str:
        """
        Converts a detailed query into an aggregate summary query.
        Finds the main FROM and WHERE clauses, even in complex WITH queries.
        Dynamically adjusts summary context (e.g., Top Product vs Top Company)
        based on the original query's GROUP BY clause.
        
        --- NEW: This version is highly optimized to avoid slow correlated subqueries. ---
        """
        
        query_to_search = original_query
        
        # --- Handle WITH clauses ---
        cte_match = re.search(r'\bWITH\b.*?\)\s*(SELECT)', original_query, re.IGNORECASE | re.DOTALL)
        if cte_match:
            main_select_start = cte_match.start(1) 
            query_to_search = original_query[main_select_start:]
            print(f"--- Debug: Complex query. Searching in main block: {query_to_search[:100]}...")

        # Extract table name and alias (e.g., "FROM EximExport e")
        table_match = re.search(r'FROM\s+([\w\.]+)(?:(?:\s+as\s+|\s+)(\w+))?', query_to_search, re.IGNORECASE)
        
        if not table_match:
            print("--- Debug: Summary query could not find FROM clause. ---")
            return None
            
        table_name = table_match.group(1) # e.g., "EximExport"
        table_alias = table_match.group(2) # e.g., "e"
        
        # --- Validate the alias ---
        if table_alias and table_alias.upper() in ['WHERE', 'GROUP', 'ORDER', 'HAVING']:
            print(f"--- Debug: Invalid alias '{table_alias}' found. Setting to None. ---")
            table_alias = None

        # Extract WHERE clause
        where_match = re.search(r'(WHERE\s+.+?)(?:GROUP BY|ORDER BY|UNION|$)', query_to_search, re.IGNORECASE | re.DOTALL)
        where_clause = where_match.group(1).strip() if where_match else ""

        # --- Remove any trailing semicolons ---
        if where_clause.endswith(';'):
            where_clause = where_clause[:-1].strip()

        # --- Optimize date functions in the extracted WHERE clause ---
        def optimize_date_sargable(match):
            col = match.group(1) # e.g., SB_Date or e.SB_Date
            op = match.group(2)  # e.g., >=
            year_func = match.group(3) # e.g., YEAR(GETDATE()) - 2 or 2024
            return f" {col} {op} DATEFROMPARTS({year_func}, 1, 1) "

        sargable_pattern_gte = r'YEAR\(([\w\.]+)\)\s*([>=]+)\s*((?:YEAR\(GETDATE\(\)\)\s*[-+]\s*\d+)|(?:YEAR\(GETDATE\(\)\))|(?:\d{4}))'
        
        if where_clause:
            optimized_where = re.sub(
                sargable_pattern_gte, 
                optimize_date_sargable, 
                where_clause, 
                flags=re.IGNORECASE
            )
            
            if optimized_where != where_clause:
                print(f"--- Debug: Optimized WHERE clause from '{where_clause}' to '{optimized_where}' ---")
                where_clause = optimized_where
            
            if table_alias:
                alias_pattern = r'\b' + re.escape(table_alias) + r'\.'
                where_clause = re.sub(alias_pattern, '', where_clause, flags=re.IGNORECASE)
                print(f"--- Debug: Stripped alias '{table_alias}' from WHERE: {where_clause}")
        
        # --- Determine which columns to use based on table ---
        if 'import' in table_name.lower():
            date_col = 'BE_Date'
        else:
            date_col = 'SB_Date'
        
        # UPDATE: The default name column is now the combined column in the View
        default_name_col = '[Importer/Exporter_Name]' 
        
        # Find the primary GROUP BY column from the original query
        group_by_match = re.search(r'GROUP BY\s+(?:[\w\.]+\.)?(\[?[\w\/\s]+\]?)', query_to_search, re.IGNORECASE)
        group_by_col = group_by_match.group(1) if group_by_match else None

        # Set the name_col and entity_name based on the GROUP BY
        if group_by_col and 'product' in group_by_col.lower():
            name_col = 'Product_Name'
            entity_name = 'Product'
        elif group_by_col and 'formatted' in group_by_col.lower():
            name_col = '[Formatted_Name]'
            entity_name = 'Company'
        elif group_by_col and 'importer/exporter' in group_by_col.lower():
            name_col = '[Importer/Exporter_Name]'
            entity_name = 'Company'
        else:
            name_col = default_name_col
            entity_name = 'Company'
            
        print(f"--- Debug: Summarizing by {entity_name} ({name_col}) ---")

        # --- START NEW, EFFICIENT V2 SUMMARY QUERY ---
        # This version uses two CTEs and a CROSS JOIN to be highly performant.
        # It avoids all slow correlated subqueries in the SELECT list.
        summary_query = f"""
        WITH TopEntity AS (
            SELECT TOP 1
                {name_col},
                SUM(Total_Value_INR) AS TopEntityValue
            FROM {table_name}
            {where_clause}
            GROUP BY {name_col}
            ORDER BY TopEntityValue DESC
        ),
        Aggregates AS (
            SELECT
                COUNT(*) as TotalRecords,
                COUNT(DISTINCT {name_col}) as Total{entity_name}s,
                SUM(Total_Value_INR) as TotalValue_INR,
                SUM(QUANTITY_KG) as TotalQuantity_KG,
                SUM(Total_Value_INR) / NULLIF(SUM(QUANTITY_KG), 0) as WeightedAvgPrice_INR,
                MAX(Total_Value_INR) as MaxShipmentValue,
                MIN({date_col}) as EarliestDate,
                MAX({date_col}) as LatestDate,
                COUNT(DISTINCT CAST({date_col} AS DATE)) as UniqueDates
            FROM {table_name}
            {where_clause}
        )
        SELECT
            a.*, -- All aggregates
            t.{name_col} as Top{entity_name},
            t.TopEntityValue as Top{entity_name}Value
        FROM Aggregates a
        CROSS JOIN TopEntity t
        """
        # --- END NEW, EFFICIENT V2 SUMMARY QUERY ---
        
        print(f"--- Debug: Generated summary query (V2): {summary_query.replace(chr(10), ' ')}")
        return summary_query.strip()
    
    # ============================================================
    # HELPER METHOD: Generate Analytical Insights with Summary Stats
    # ============================================================
    def _generate_insights(self, user_query, viz_data, summary_stats):
        """
        Creates business insights by analyzing:
        1. Summary statistics (accurate aggregates from ALL rows)
        2. Representative sample data (for qualitative context)
        """
        
        if not summary_stats or len(summary_stats) == 0:
            print("--- No summary stats available, skipping insights ---")
            return None
        
        if not viz_data or len(viz_data) == 0:
            return None
        
        stats = summary_stats[0]
        
        # --- START NEW DYNAMIC & SAFE LOGIC ---
        
        # Helper to safely format numbers (prevents "Cannot specify ',' with 's'" error)
        def safe_format(key, format_spec):
            val = stats.get(key, 'N/A')
            if isinstance(val, (int, float)):
                return f"{val:{format_spec}}"
            return str(val) # Return 'N/A' or other strings as-is

        # Dynamically determine the entity (Product vs Company)
        entity_name = "Product" if f'TotalProducts' in stats else "Company"
        
        top_entity_key = f'Top{entity_name}'
        raw_top_entity_name = stats.get(top_entity_key, 'N/A')
        
        sanitized_top_entity_name = str(raw_top_entity_name) # Ensure string
        if len(sanitized_top_entity_name) > 75:
            sanitized_top_entity_name = sanitized_top_entity_name[:75] + "..."

        # Prepare summary statistics for LLM
        summary_context = f"""
COMPLETE DATASET STATISTICS (from all {safe_format('TotalRecords', ',')} records):
- Total Records: {safe_format('TotalRecords', ',')}
- Total {entity_name}s: {safe_format(f'Total{entity_name}s', ',')}
- Total Value: ₹{safe_format('TotalValue_INR', ',.2f')}
- Total Quantity: {safe_format('TotalQuantity_KG', ',.2f')} KG
- Weighted Average Price: ₹{safe_format('WeightedAvgPrice_INR', ',.2f')} per KG
- Highest Single Shipment: ₹{safe_format('MaxShipmentValue', ',.2f')}
- Date Range: {safe_format('EarliestDate', 's')} to {safe_format('LatestDate', 's')}
- Top {entity_name}: {safe_format(f'Top{entity_name}', 's')} (₹{safe_format(f'Top{entity_name}Value', ',.2f')})
- Unique Trading Days: {safe_format('UniqueDates', ',')}
"""
        # --- END NEW DYNAMIC & SAFE LOGIC ---
        
        # Prepare sample data (limited for context)
        # sample_summary = json.dumps(viz_data[:20], ensure_ascii=False, indent=2, default=str)
        # if len(viz_data) > 20:
        #     sample_summary += f"\n... (showing top 20 of {len(viz_data)} companies for context)"
        
        # --- START SANITIZED SAMPLE BLOCK --- (Sanitizing names for insights generation)
        # Dynamically find the name column (e.g., 'Product_Name', 'Importer_Name')
        name_col = next((key for key in viz_data[0].keys() if 
                         key.lower() in ['product_name', 'importer/exporter_name', 'formatted_name', 'product']), None)
        
        sample_summary = ""
        if name_col:
            # Get top 10 names from the (already sorted) viz_data
            top_names = [row[name_col] for row in viz_data[:10]]
            
            # Sanitize by truncating long names
            sanitized_names = []
            for name in top_names:
                name_str = str(name) # Ensure it's a string
                if len(name_str) > 75:
                    sanitized_names.append(f"- {name_str[:75]}...")
                else:
                    sanitized_names.append(f"- {name_str}")
            
            sample_summary = "\n".join(sanitized_names)
            if len(viz_data) > 10:
                sample_summary += f"\n\n... (showing top 10 of {len(viz_data)} results)"
        else:
            print("--- Warning: Could not find name column for sample summary. ---")
        # --- END SANITIZED SAMPLE BLOCK ---

        # Prompt LLM to analyze with both statistical and qualitative data
        insight_prompt = f"""
                            You are a business analyst reviewing import/export data. The user asked: "{user_query}"

                            Here is the complete statistical summary for the data matching that query:
                            {summary_context}

                            Here are the top results from the data (for qualitative context):
                            {sample_summary}

                            CRITICAL: Your analysis MUST use the "COMPLETE DATASET STATISTICS" above for all numerical claims.
                            The "top results" list is only for qualitative context and may be incomplete.

                            Please provide a concise, professional analysis (3-5 sentences) that includes:
                            1. **Scale & Scope**: Comment on the overall market size using the COMPLETE statistics
                            2. **Key Players**: Highlight the top {entity_name} (from the stats) and use the "top results" list for context.
                            3. **Pricing Insights**: Discuss the weighted average price and any notable patterns
                            4. **Business Implications**: What do these numbers mean for the market?

                            Format your response as natural paragraphs (no bullet points). Use specific numbers from the COMPLETE DATASET STATISTICS.

                            Example format:
                            "The zinc import market shows significant activity with [X] companies collectively importing ₹[Y] worth of materials. [Top Company] dominates with [Z%] market share. The weighted average price of ₹[W]/kg suggests [insight]. This indicates [business implication]."
                            """
        
        try:
            print("--- Generating analytical insights with full dataset stats ---")
            insight_response = self.model.generate_content(insight_prompt)
            insights = insight_response.text.strip()
            return insights
        except Exception as e:
            print(f"Error generating insights: {e}")
            return None
        
    # ============================================================
    # HELPER METHOD: Detect and Respond to Small Talk
    # ============================================================
    def _detect_smalltalk(self, user_query: str):
        """Identifies casual conversation and returns friendly responses"""
        query = user_query.strip().lower()

        # Define small talk categories
        greetings = ["hi", "hello", "hey", "good morning", "good afternoon", "good evening", "hola"]
        gratitude = ["thanks", "thank you", "appreciate", "great job", "cool", "awesome"]
        wellbeing = ["how are you", "how's it going", "how are things", "how are you doing"]
        
        # UPDATED LIST BELOW: Added "what do you do", "capabilities", "features"
        help_queries = [
            "help", "what can you do", "who are you", "what is this", 
            "what do you do", "capabilities", "features", "function", 
            "what is your purpose"
        ]

        # Match and respond to small talk
        if any(re.search(r"\b" + re.escape(word) + r"\b", query) for word in greetings):
            return "Hello there! 👋 I'm your analytics assistant. Ask me about import trends, totals, or comparisons."
        elif any(re.search(r"\b" + re.escape(word) + r"\b", query) for word in wellbeing):
            return "I'm doing great — thanks for asking! 😊 How can I help you explore your import data today?"
        elif any(re.search(r"\b" + re.escape(word) + r"\b", query) for word in gratitude):
            return "You're welcome! Happy to help anytime. 🙌"
        elif any(re.search(r"\b" + re.escape(word) + r"\b", query) for word in help_queries):
            return ("I am an AI Data Agent designed to analyze your Import/Export data.<br><br>"
                    "I can help you with:<br>"
                    "• **Data Retrieval:** 'Show me full export data for Zinc.'<br>"
                    "• **Analysis:** 'Who are the top 5 importers?'<br>"
                    "• **Comparison:** 'Compare air vs sea shipments.'")
        else:
            return None

    # ============================================================
    # MAIN METHOD: Process User Query and Return Response
    # ============================================================
    def ask(self, user_query, history=[]):
        """Processes natural language queries and returns SQL results with insights"""
        
        # ============================================================
        # STEP 1: Check for Small Talk (Skip LLM if Casual Chat)
        # ============================================================
        smalltalk_response = self._detect_smalltalk(user_query)
        if smalltalk_response:
            return {
                "answer": smalltalk_response,
                "data": [],
                "query": "",
                "query_type": "analytical", # Use a default type
                "chart_title": "",
                "is_time_series": False
            }
        
        # ============================================================
        # STEP 2: Format Conversation History for Context
        # ============================================================
        formatted_history = ""
        for turn in history:
            role = "User" if turn.get('role') == 'user' else "Agent"
            content = turn.get('content', '')
            formatted_history += f"{role}: {content}\n"

        # ============================================================
        # STEP 3: Build LLM Prompt with Schema and Instructions
        # ============================================================
        prompt = f"""
        You are an expert SQL database analyst. Your task is to help a user
        get insights from their import/export tables.
        You must follow the rules below perfectly.

        Here is the database schema you are working with:
        --- START SCHEMA ---
        {self.schema}
        --- END SCHEMA ---

        --- CRITICAL: TABLE SELECTION RULES ---
        You have TWO views. You must choose the correct one based on the user's query:
        
        1.  **`View_Clean_Imports` (IMPORTS View):**
            * Use this for all questions about **IMPORTS**, "import data", "buying", "purchases", "bill of entry", etc.
            * Main Date Column: `BE_Date`.
            * Company Name Column: `[Importer/Exporter_Name]` (This contains the Importer).
        
        2.  **`View_Clean_Exports` (EXPORTS View):**
            * Use this for all questions about **EXPORTS**, "export data", "shipping bills", "selling", "shipments", etc.
            * Main Date Column: `SB_Date`.
            * Company Name Column: `[Importer/Exporter_Name]` (This contains the Exporter).

        If the user explicitly says "from imports" or "from imports table", you MUST use `View_Clean_Imports`.
        If the user explicitly says "from exports" or "from exports table", you MUST use `View_Clean_Exports`.
        Do not confuse the two.

        --- CRITICAL: COLUMN USAGE RULES ---
        1. **Company Names:** ALWAYS use `[Importer/Exporter_Name]`. 
           - NEVER use 'Importer_Name' or 'Exporter_Name' directly.
           - Example: `SELECT [Importer/Exporter_Name], SUM(Total_Value_INR)...`
        2. **Short Names:** If the user asks for "Formatted Name" or "Short Name", use `[Formatted_Name]`.
        3. **Product Names:** Continue to use `Product_Name` for full names or `Product` for standardized names.
        4. **Dates:** Use `BE_Date` for Imports and `SB_Date` for Exports.

        --- START CONVERSATION HISTORY ---
        {formatted_history}
        --- END CONVERSATION HISTORY ---

        Based on the history, and most importantly the user's NEWEST question,
        generate the correct SQL and answer.

        The user's NEWEST question is:
        "{user_query}"

        --- RESPONSE LOGIC: 'query_type' ---
        Determine the user's intent and set ONE of the following `query_type` values:

        1.  **`query_type: "analytical"` (DEFAULT)**
            * This is for *most* questions: "top 5 importers," "analyze companies," "what's the total value of all exports?".
            * These queries aggregate data and are perfect for charts.
            * The answer should be a 1-sentence introduction (e.g., "Here is the analysis of...").
            * Use `TOP 15` for broad questions (e.g., "analyze companies") to keep charts readable.
            * For "what's the total value?", generate a query like `SELECT SUM(Total_Value_INR) as TotalValue FROM EximExport`. This will produce a 1x1 table.

        2.  **`query_type: "comparison"`**
            * Use this for direct comparisons: "compare air vs sea," "product A vs product B," "rate comparison...".
            * These queries are also perfect for charts.
            * The answer should be a 1-sentence introduction.
            * You can include a more complex analysis in the answer if it's simple (e.g., "Air freight is 20% higher than sea freight.").

        3.  **`query_type: "data_pull"`**
            * Use this *only* if the user asks for "list," "table," "show me the data," "full list," "full data," or "raw data."
            * These queries are for viewing raw data, not for charts.
            * The answer should be a 1-sentence introduction (e.g., "Here is the raw data you requested.").
            * Use `SELECT TOP 50 *` by default.
            * If the user asks for "full list," "all data," "entire list," or "full data", you MUST remove the `TOP 50` limit (e.g., `SELECT * FROM ...`).

        --- QUERY GENERATION RULES ---
        - Always enclose special column names in square brackets (e.g., [Total_Value_INR]).
        - When filtering by names, use `Product_Name LIKE '%searchterm%'`. The system handles normalization.
        - For cost/value queries, default to 'Total_Value_INR'.

        --- CRITICAL: CATEGORY ANALYSIS RULE (FOR "ANALYZE" AND "COMPARE") ---
        - If the user asks to "analyze" OR "compare" a *broad category* (e.g., "analyze zinc products," "compare dimerfattyacid rates," "show me steel rates"), you MUST NOT summarize it into a single row.
        - You MUST generate a query that `GROUP BY Product_Name` (or `Importer_Name`, etc.) and filters for that category in the `WHERE` clause.
        - This query must select the `Product_Name` column and the requested metrics.
        
        - ❌ BAD (Single Row): `SELECT 'Zinc' AS Product, SUM(...) ... WHERE Product_Name LIKE '%zinc%'`
        
        - ✅ GOOD (Multiple Rows, Grouped by Actual Name):
          ```sql
          SELECT
              Product_Name, -- The REAL product name
              SUM(CASE WHEN [BE_Date] >= DATEFROMPARTS(YEAR(GETDATE()), 1, 1) ... END) AS CurrentYearValue_INR,
              SUM(CASE WHEN [BE_Date] >= DATEFROMPARTS(YEAR(GETDATE()) - 1, 1, 1) ... END) AS LastYearValue_INR
          FROM EximImport
          WHERE [Product] LIKE '%DIMERFATTYACID%'
            AND [BE_Date] >= DATEFROMPARTS(YEAR(GETDATE()) - 1, 1, 1)
          GROUP BY Product_Name
          ORDER BY CurrentYearValue_INR DESC;
          ```
        - The `query_type` for this should be "comparison" or "analytical".

        --- CRITICAL: SINGLE-ENTITY TIME COMPARISON RULE ---
        - If the user asks to "compare" a *single specific entity* (like "Nutracare International") between time periods (e.g., "last year and this year"), DO NOT pivot the data into columns (e.g., `SUM(CASE...) AS CurrentYear`).
        - You MUST return multiple rows, one for each period, using a `UNION ALL`.
        - You MUST create a 'Period' column. This will create a correct pie chart.
        
        - ❌ BAD (Single Row, Pivoted):
          `SELECT 'Nutracare International' AS Importer, SUM(CASE...) AS Current, SUM(CASE...) AS Last FROM EximImport WHERE Importer LIKE '%NUTRACARE%';`
          
        - ✅ GOOD (Multiple Rows, Unpivoted):
          ```sql
          SELECT
              'Current Year' AS Period,
              SUM(Total_Value_INR) AS TotalImportValue
          FROM EximImport
          WHERE Importer LIKE '%NUTRACARE%' AND [BE_Date] >= DATEFROMPARTS(YEAR(GETDATE()), 1, 1) AND [BE_Date] < DATEFROMPARTS(YEAR(GETDATE()) + 1, 1, 1)
          UNION ALL
          SELECT
              'Last Year' AS Period,
              SUM(Total_Value_INR) AS TotalImportValue
          FROM EximImport
          WHERE Importer LIKE '%NUTRCARE%' AND [BE_Date] >= DATEFROMPARTS(YEAR(GETDATE()) - 1, 1, 1) AND [BE_Date] < DATEFROMPARTS(YEAR(GETDATE()), 1, 1)
          ```
        
        --- CRITICAL: DATE FILTERING RULE (PERFORMANCE) ---
        - To filter by date, NEVER apply a function (like YEAR()) to the date column in the WHERE clause or a CASE statement.
        - This forces a full table scan and is extremely slow.
        
        - ❌ BAD (SLOW): `WHERE YEAR(SB_Date) = 2024`
        - ✅ GOOD (FAST): `WHERE SB_Date >= '2024-01-01' AND SB_Date < '2025-01-01'`
        
        - ❌ BAD (SLOW): `SUM(CASE WHEN YEAR(SB_Date) = 2024 THEN ... END)`
        - ✅ GOOD (FAST): `SUM(CASE WHEN SB_Date >= '2024-01-01' AND SB_Date < '2025-01-01' THEN ... END)`

        - ❌ BAD (SLOW): `WHERE YEAR(SB_Date) = YEAR(GETDATE()) - 1`
        - ✅ GOOD (FAST): `WHERE SB_Date >= DATEFROMPARTS(YEAR(GETDATE()) - 1, 1, 1) AND SB_Date < DATEFROMPARTS(YEAR(GETDATE()), 1, 1)`

        - ❌ BAD (SLOW): `SUM(CASE WHEN YEAR(SB_Date) = YEAR(GETDATE()) - 1 THEN ... END)`
        - ✅ GOOD (FAST): `SUM(CASE WHEN SB_Date >= DATEFROMPARTS(YEAR(GETDATE()) - 1, 1, 1) AND SB_Date < DATEFROMPARTS(YEAR(GETDATE()), 1, 1) THEN ... END)`
        
        --- CRITICAL: TOP N QUERY RULE ---
        When a user asks for "Top N" items (like "top 10 products"), you MUST use a Common Table Expression (CTE) to find the Top N items first, then join against it. This is much faster.
        Example of an EFFICIENT Top N Query for "top 10 products rate comparison":
        
        WITH TopProducts AS (
            SELECT TOP 10 Product_Name, SUM(Total_Value_INR) AS OverallValue
            FROM EximExport
            WHERE SB_Date >= DATEFROMPARTS(YEAR(GETDATE()) - 2, 1, 1) -- SARGable WHERE
            GROUP BY Product_Name
            ORDER BY OverallValue DESC
        )
        SELECT
            e.Product_Name,
            
            -- Current Year Rate (SARGable CASE)
            SUM(CASE WHEN e.SB_Date >= DATEFROMPARTS(YEAR(GETDATE()), 1, 1) AND e.SB_Date < DATEFROMPARTS(YEAR(GETDATE()) + 1, 1, 1)
                     THEN e.Total_Value_INR ELSE 0 END) 
            / 
            NULLIF(SUM(CASE WHEN e.SB_Date >= DATEFROMPARTS(YEAR(GETDATE()), 1, 1) AND e.SB_Date < DATEFROMPARTS(YEAR(GETDATE()) + 1, 1, 1)
                            THEN e.QUANTITY_KG ELSE 0 END), 0) 
            AS CurrentYearAvgRate_INR,

            -- Last Year Rate (SARGable CASE)
            SUM(CASE WHEN e.SB_Date >= DATEFROMPARTS(YEAR(GETDATE()) - 1, 1, 1) AND e.SB_Date < DATEFROMPARTS(YEAR(GETDATE()), 1, 1)
                     THEN e.Total_Value_INR ELSE 0 END) 
            / 
            NULLIF(SUM(CASE WHEN e.SB_Date >= DATEFROMPARTS(YEAR(GETDATE()) - 1, 1, 1) AND e.SB_Date < DATEFROMPARTS(YEAR(GETDATE()), 1, 1)
                            THEN e.QUANTITY_KG ELSE 0 END), 0) 
            AS LastYearAvgRate_INR,

            -- Two Years Ago Rate (SARGable CASE)
            SUM(CASE WHEN e.SB_Date >= DATEFROMPARTS(YEAR(GETDATE()) - 2, 1, 1) AND e.SB_Date < DATEFROMPARTS(YEAR(GETDATE()) - 1, 1, 1)
                     THEN e.Total_Value_INR ELSE 0 END) 
            / 
            NULLIF(SUM(CASE WHEN e.SB_Date >= DATEFROMPARTS(YEAR(GETDATE()) - 2, 1, 1) AND e.SB_Date < DATEFROMPARTS(YEAR(GETDATE()) - 1, 1, 1)
                            THEN e.QUANTITY_KG ELSE 0 END), 0) 
            AS TwoYearsAgoAvgRate_INR
            
        FROM EximExport e
        INNER JOIN TopProducts tp ON e.Product_Name = tp.Product_Name
        WHERE e.SB_Date >= DATEFROMPARTS(YEAR(GETDATE()) - 2, 1, 1) -- SARGable WHERE
        GROUP BY e.Product_Name
        ORDER BY MAX(tp.OverallValue) DESC;
        
        --- CRITICAL: WEIGHTED AVERAGE CALCULATION RULE ---
        WHENEVER calculating average price, you MUST use weighted average:
        ✅ CORRECT: SUM([Total_Value_INR]) / NULLIF(SUM([Quantity_KG]), 0) AS WeightedAvgPrice_INR
        **CRITICAL: Always use NULLIF to prevent divide-by-zero errors!**

        --- CRITICAL: 'is_time_series' RULE ---
        You MUST set "is_time_series": true if the query groups data "over time," "by date," "monthly," "daily,", "yearly", or if the user asks for "trend", "trend analysis" etc.
        Otherwise, set "is_time_series": false.

        --- OUTPUT FORMAT ---
        Return a single, valid JSON object with these keys:
        - "sql_query": The T-SQL query you generated.
        - "answer": Brief 1-sentence introduction.
        - "query_type": "analytical" | "comparison" | "data_pull"
        - "is_time_series": true | false
        - "chart_title": Descriptive title for charts (e.g., "Top 15 Zinc Importers by Value").
        
        Example (Analytical - Top 15):
        {{
        "sql_query": "SELECT TOP 15 Importer_Name, SUM([Total_Value_INR])/SUM([QUANTITY_KG]) AS WeightedAvgPrice_INR, SUM([QUANTITY_KG]) AS TotalVolume FROM EximImport WHERE Product_Name LIKE '%zinc%' GROUP BY Importer_Name ORDER BY TotalVolume DESC",
        "answer": "Here are the top 15 companies importing zinc products:",
        "query_type": "analytical",
        "is_time_series": false,
        "chart_title": "Top 15 Zinc Importers - Price & Volume Analysis"
        }}
        
        Example (Data Pull - Full List):
        {{
        "sql_query": "SELECT * FROM EximImport WHERE Product_Name LIKE '%dimerfattyacid%' AND BE_Date >= '2024-01-01'",
        "answer": "Here is the full list of imports for dimerfattyacid from Jan 2024:",
        "query_type": "data_pull",
        "is_time_series": false,
        "chart_title": "Full Import List: dimerfattyacid"
        }}
        """

        try:
            # ============================================================
            # STEP 4: Generate SQL Query Using LLM
            # ============================================================
            print("--- Generating SQL from LLM ---")
            llm_response = self.model.generate_content(prompt)
            
            # ============================================================
            # STEP 5: Safety Check - Handle Empty/Invalid LLM Responses
            # ============================================================
            try:
                if not llm_response.candidates or not llm_response.candidates[0].content.parts:
                    print("⚠️ Empty response from Gemini, retrying once...")
                    llm_response = self.model.generate_content(prompt)
                    if not llm_response.candidates or not llm_response.candidates[0].content.parts:
                        print("⚠️ Gemini returned empty response even after retry.")
                        return {"answer": "I'm sorry, I couldn't process that question right now. Please try rephrasing it.", "data": [], "query": ""}
                
                llm_text = getattr(llm_response, "text", "").strip()
                if not llm_text:
                    print("⚠️ Gemini produced no text output.")
                    return {"answer": "I couldn't generate a response for that question. Please try again.", "data": [], "query": ""}

                cleaned_json_str = self._clean_llm_response(llm_text)

            except Exception as inner_e:
                print(f"⚠️ Error accessing Gemini response: {repr(inner_e)}")
                return {"answer": "I'm sorry, I ran into a temporary issue while processing your question.", "data": [], "query": ""}

            # ============================================================
            # STEP 6: Parse LLM Response as JSON
            # ============================================================
            if not cleaned_json_str:
                raise Exception(f"LLM did not return valid JSON. Response: {repr(llm_response.text)}")

            llm_data = json.loads(cleaned_json_str)
            sql_query = llm_data.get("sql_query")
            answer = llm_data.get("answer", "I found some data for you.")
            chart_title = llm_data.get("chart_title", "")
            is_time_series = llm_data.get("is_time_series", False)
            query_type = llm_data.get("query_type", "analytical")

            # ============================================================
            # STEP 6.5: Fix Product Column Usage in SQL (NEW!)
            # ============================================================
            if sql_query:
                original_query = sql_query
                sql_query = self._fix_product_column_in_sql(sql_query)
                if original_query != sql_query:
                    print(f"🔧 Original SQL: {original_query}")
                    print(f"✅ Fixed SQL: {sql_query}")

            # ============================================================
            # STEP 7: Log Query Classification for Debugging
            # ============================================================
            
            print("\n================= 🧠 LLM Classification =================")
            # --- NEW LOGGING ---
            print(f"🧩 Query Type: {query_type.upper()}")
            print(f"📜 SQL Generated: {sql_query}")
            print("=========================================================\n")

            if not sql_query:
                return {"answer": "I'm sorry, I couldn't generate a SQL query for that.", "data": [], "query": "N/A"}

            # ============================================================
            # STEP 8: Execute SQL Query with Auto-Retry Fallback
            # ============================================================
            print(f"--- Executing SQL safely ---")
            
            def try_execute_sql(query, conn):
                try:
                    result = conn.execute(text(query))
                    return result
                except Exception as e:
                    if "Invalid column" in str(e) or "Syntax" in str(e):
                        print("⚠️ LLM might have used '=' instead of LIKE. Retrying with LIKE...")
                        query_like = re.sub(r"(Importer_Name|Exporter_Name|Product_Name)\s*=\s*'([^']+)'", r"\1 LIKE '%\2%'", query)
                        print(f"🔄 Retrying modified SQL:\n{query_like}\n")
                        return conn.execute(text(query_like))
                    else:
                        raise

            with self.engine.connect() as conn:
                try:
                    result = try_execute_sql(sql_query, conn)
                    rows = result.fetchall()
                    column_names = list(result.keys())

                    formatted_rows = []
                    for row in rows:
                        new_row_dict = {}
                        for i, val in enumerate(row):
                            col_name = column_names[i]
                            if isinstance(val, (datetime.date, datetime.datetime)):
                                new_row_dict[col_name] = val.strftime('%d-%b-%Y')
                            elif col_name in ('BE_Number', 'SB_Number') and val is not None:
                                try:
                                    new_row_dict[col_name] = f"{float(val):.0f}"
                                except (ValueError, TypeError):
                                    new_row_dict[col_name] = str(val)
                            else:
                                new_row_dict[col_name] = val
                        formatted_rows.append(new_row_dict)
                    data_for_viz = formatted_rows

                    summary_query = self._generate_summary_query(sql_query)
                    summary_stats = None
                    if summary_query:
                        summary_result = try_execute_sql(summary_query,conn)
                        summary_rows = summary_result.fetchall()
                        summary_column_names = summary_result.keys()
                        summary_stats = [dict(zip(summary_column_names, row)) for row in summary_rows]
                    else:
                        print("--- Skipping summary query (could not be generated) ---")

                    row_count = len(data_for_viz)
                    if row_count > 10000:   # Handle LARGE DATASETS
                        job_id = str(int(time.time()))
                        export_jobs[job_id] = {"status": "processing", "progress": 0, "file": None}
                        def export_job():
                            try:
                                export_dir = "exports"
                                os.makedirs(export_dir, exist_ok=True)
                                filename = f"export_{job_id}.xlsx"
                                file_path = os.path.join(export_dir, filename)
                                df = pd.DataFrame(data_for_viz)
                                with pd.ExcelWriter(file_path, engine='xlsxwriter', engine_kwargs={'options': {'strings_to_formulas': False}}) as writer:
                                    df.to_excel(writer, index=False, sheet_name='Data')
                                    workbook = writer.book
                                    worksheet = writer.sheets['Data']
                                    text_format = workbook.add_format({'num_format': '@'})
                                    headers = df.columns.tolist()
                                    try:
                                        be_col_idx = headers.index('BE_Number')
                                        worksheet.set_column(be_col_idx, be_col_idx, None, text_format)
                                    except ValueError: pass
                                    try:
                                        hs_col_idx = headers.index('HS_Code')
                                        worksheet.set_column(hs_col_idx, hs_col_idx, None, text_format)
                                    except ValueError: pass
                                export_jobs[job_id]["status"] = "ready"
                                export_jobs[job_id]["progress"] = 100
                                export_jobs[job_id]["file"] = filename
                            except Exception as e:
                                export_jobs[job_id]["status"] = "error"
                                print("Export error:", e)
                        Thread(target=export_job).start()
                        
                        # --- Pass query_type to insights ---
                        insights = self._generate_insights(user_query, data_for_viz, summary_stats)

                        return {
                            "answer": f"{answer}\n\n{insights} \n\n⏳ The dataset contains **{row_count:,} rows**. I am preparing a downloadable Excel file...",
                            "data": [],
                            "query": sql_query,
                            "chart_title": chart_title,
                            "export_job_id": job_id,
                            # --- NEW RETURN KEYS ---
                            "query_type": query_type,
                            "is_time_series": is_time_series
                        }

                except Exception as sql_error:
                    print(f"❌ SQL Execution Error: {sql_error}")
                    return {"answer": "I couldn't run the generated SQL query correctly. Please rephrase your question or try again.", "data": [], "query": sql_query}

            # ============================================================
            # STEP 9: Post-Process Results Based on Query Type
            # ============================================================
            
            print(f"--- Generating insights for {query_type} query ---")
            insights = self._generate_insights(user_query, data_for_viz, summary_stats)

            if insights:
                answer = f"{answer}\n\n{insights}"
            
            if not data_for_viz:
                print("--- Data result was empty. Overriding answer. ---")
                # Provide a more helpful "no data found" message
                answer = f"I've looked for the data you requested:\n`{user_query}`\nHowever, I couldn't find any matching records in the database."

            # ============================================================
            # STEP 10: Return Structured Response
            # ============================================================
            return {
                "answer": answer,
                "data": data_for_viz,
                "query": sql_query,
                "chart_title": chart_title,
                "query_type": query_type, 
                "is_time_series": is_time_series
            }

        except Exception as e:
            print(f"Error in agent.ask: {e}")
            return {
                "answer": "I'm sorry, I encountered a temporary issue while generating that insight. Please try rephrasing your question.",
                "data": [],
                "query": ""
            }