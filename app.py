# app.py
import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime
import os
import json
from openai import AzureOpenAI

from dotenv import load_dotenv

load_dotenv()

# Page configuration
st.set_page_config(
    page_title="Georgia Water Safety Explorer",
    page_icon="💧",
    layout="wide",
    initial_sidebar_state="expanded"
)

class WaterSystemExplorer:
    """Main class for water system data exploration"""
    
    def __init__(self, db_path="sdwis_georgia.db"):
        self.db_path = db_path
        self.health_info = self._init_health_info()
        self.violation_explanations = self._init_violation_explanations()
        self.azure_client = self._init_azure_openai("high")
        self.azure_client_low = self._init_azure_openai("low")
    
    def get_connection(self):
        """Get database connection"""
        return sqlite3.connect(self.db_path)
    
    def execute_query(self, query, params=None):
        """Execute SQL query and return DataFrame"""
        try:
            with self.get_connection() as conn:
                return pd.read_sql_query(query, conn, params=params or {})
        except Exception as e:
            st.error(f"Database error: {str(e)}")
            return pd.DataFrame()
    
    def execute_generated_query(self, query):
        """Execute a generated SQL query and return results"""
        try:
            with self.get_connection() as conn:
                return pd.read_sql_query(query, conn)
        except Exception as e:
            st.error(f"Query execution error: {str(e)}")
            return pd.DataFrame()
    
    def generate_sql_query(self, natural_language_query):
        """Generate SQL query from natural language using AI"""
        if not self.azure_client_low:
            st.error("AI service not available for query generation")
            return None
        
        try:
            # Get database schema information
            schema_info = self._get_database_schema()
            
            prompt = f"""You are a SQL expert specializing in water safety data analysis.
            
Database Schema:
{schema_info}

Natural Language Query: {natural_language_query}

Generate a SQL query that answers the user's question. The query should:
1. Be safe and read-only (SELECT statements only)
2. Use proper JOIN statements when needed
3. Include relevant columns for water safety analysis
4. Limit results to a reasonable number (e.g., LIMIT 100)
5. Return results that can be displayed in a water safety context

Important tables:
- pub_water_systems: Basic water system information
- violations_enforcement: Violation records
- lcr_samples: Test results
- geographic_areas: Location information

Return ONLY the SQL query without any explanation or formatting."""

            response = self.azure_client_low.chat.completions.create(
                model=os.getenv('AZURE_OPENAI_DEPLOYMENT_NAME_2'),
                messages=[
                    {"role": "system", "content": "You are a SQL expert who generates safe, read-only queries for water safety data analysis."},
                    {"role": "user", "content": prompt}
                ],
                max_completion_tokens=1000,
                temperature=0.1
            )
            
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            st.error(f"Failed to generate SQL query: {str(e)}")
            return None
    
    def _get_database_schema(self):
        """Get basic database schema information"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Get table names
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = cursor.fetchall()
                
                schema_info = "Database Tables and Key Columns:\n\n"
                
                for table in tables:
                    table_name = table[0]
                    cursor.execute(f"PRAGMA table_info({table_name})")
                    columns = cursor.fetchall()
                    
                    schema_info += f"{table_name}:\n"
                    for col in columns[:10]:  # Limit to first 10 columns
                        schema_info += f"  - {col[1]} ({col[2]})\n"
                    schema_info += "\n"
                
                return schema_info
        except Exception as e:
            return f"Error getting schema: {str(e)}"
    
    def _init_health_info(self):
        """Initialize contaminant health information"""
        return {
            'LEAD': {
                'health_effects': 'Can cause developmental delays in children, kidney problems, and high blood pressure',
                'sources': 'Lead pipes, faucets, and fixtures; solder',
                'action_level': '15 ppb',
                'severity': 'High'
            },
            'COPPER': {
                'health_effects': 'Short-term exposure can cause gastrointestinal distress; long-term exposure can cause liver or kidney damage',
                'sources': 'Copper pipes; erosion of natural deposits',
                'action_level': '1.3 ppm',
                'severity': 'Medium'
            },
            'COLIFORM': {
                'health_effects': 'May indicate presence of harmful bacteria, viruses, or parasites',
                'sources': 'Human and animal fecal waste',
                'action_level': '0 positive samples',
                'severity': 'High'
            },
            'NITRATE': {
                'health_effects': 'Can cause blue baby syndrome in infants under 6 months',
                'sources': 'Fertilizer runoff, septic systems, erosion of natural deposits',
                'action_level': '10 ppm',
                'severity': 'High'
            }
        }
    
    def _init_violation_explanations(self):
        """Initialize violation type explanations"""
        return {
            'MCL': 'Maximum Contaminant Level - The highest level of a contaminant allowed in drinking water',
            'MRDL': 'Maximum Residual Disinfectant Level - The highest level of disinfectant allowed',
            'TT': 'Treatment Technique - Required processes to reduce contaminant levels',
            'MR': 'Monitoring and Reporting - Required testing and reporting to ensure safety',
            'MON': 'Monitoring - Required testing was not completed',
            'RPT': 'Reporting - Required reports were not submitted'
        }
    
    def _init_azure_openai(self, model_type: str="high"):
        """Initialize Azure OpenAI client"""
        try:
            if model_type == "high":
                api_key = os.getenv('AZURE_OPENAI_API_KEY')
                endpoint = os.getenv('AZURE_OPENAI_ENDPOINT')
                api_version = os.getenv('AZURE_OPENAI_API_VERSION', '2024-02-15-preview')
            else:
                api_key = os.getenv('AZURE_OPENAI_API_KEY_2')
                endpoint = os.getenv('AZURE_OPENAI_ENDPOINT_2')
                api_version = os.getenv('AZURE_OPENAI_API_VERSION_2', '2024-02-15-preview')
            
            if not api_key or not endpoint:
                st.warning("⚠️ Azure OpenAI configuration not found. AI summaries will use default text.")
                return None
            
            return AzureOpenAI(
                api_key=api_key,
                api_version=api_version,
                azure_endpoint=endpoint
            )
        except Exception as e:
            st.warning(f"⚠️ Failed to initialize Azure OpenAI: {str(e)}. Using default summaries.")
            return None
    
    def find_water_systems(self, search_term):
        """Find water systems by search term"""
        query = """
        SELECT DISTINCT p.PWSID, p.PWS_NAME, p.PWS_TYPE_CODE, p.POPULATION_SERVED_COUNT,
               p.CITY_NAME, g.COUNTY_SERVED, g.ZIP_CODE_SERVED
        FROM pub_water_systems p
        LEFT JOIN geographic_areas g ON p.PWSID = g.PWSID
        WHERE p.PWS_ACTIVITY_CODE = 'A' AND (
          UPPER(p.CITY_NAME) LIKE UPPER(:search) OR
          UPPER(g.COUNTY_SERVED) LIKE UPPER(:search) OR
          g.ZIP_CODE_SERVED LIKE :search OR
          UPPER(p.PWS_NAME) LIKE UPPER(:search)
        )
        ORDER BY p.POPULATION_SERVED_COUNT DESC
        """
        return self.execute_query(query, {'search': f'%{search_term}%'})
    
    def get_system_safety_data(self, pwsid):
        """Get comprehensive safety data for a water system"""
        queries = {
            'basic_info': """
                SELECT PWS_NAME, PWS_TYPE_CODE, POPULATION_SERVED_COUNT, 
                       PRIMARY_SOURCE_CODE, CITY_NAME, PHONE_NUMBER
                FROM pub_water_systems WHERE PWSID = :pwsid
            """,
            'recent_violations': """
                SELECT VIOLATION_CATEGORY_CODE, CONTAMINANT_CODE, 
                       NON_COMPL_PER_BEGIN_DATE, NON_COMPL_PER_END_DATE,
                       VIOLATION_STATUS, IS_HEALTH_BASED_IND
                FROM violations_enforcement 
                WHERE PWSID = :pwsid 
                AND NON_COMPL_PER_BEGIN_DATE >= date('now', '-2 years')
                ORDER BY NON_COMPL_PER_BEGIN_DATE DESC
            """,
            'test_results': """
                SELECT CONTAMINANT_CODE, SAMPLE_MEASURE, UNIT_OF_MEASURE,
                       SAMPLING_END_DATE, RESULT_SIGN_CODE
                FROM lcr_samples 
                WHERE PWSID = :pwsid 
                ORDER BY SAMPLING_END_DATE DESC
                LIMIT 10
            """
        }
        
        results = {}
        for key, query in queries.items():
            results[key] = self.execute_query(query, {'pwsid': pwsid})
        return results
    
    def _generate_summary(self, safety_data, system_info):
        """Generate a summary of the water system's safety status"""
        violations_df = safety_data['recent_violations']
        test_results_df = safety_data['test_results']
        
        # Count violations
        total_violations = len(violations_df)
        health_violations = len(violations_df[violations_df['IS_HEALTH_BASED_IND'] == 'Y']) if not violations_df.empty else 0
        unresolved_health = len(violations_df[
            (violations_df['IS_HEALTH_BASED_IND'] == 'Y') & 
            (violations_df['VIOLATION_STATUS'].isin(['Unaddressed', 'Addressed']))
        ]) if not violations_df.empty else 0
        
        # Generate population info
        population = int(system_info.get('POPULATION_SERVED_COUNT', 0))
        pop_text = f"{population:,} people" if population > 0 else "unknown number of people"
        
        # Generate fallback summary text
        if unresolved_health > 0:
            fallback_summary = f"⚠️ **ATTENTION REQUIRED**: This water system serving {pop_text} currently has {unresolved_health} active health-based violation(s). Immediate action may be needed to ensure water safety."
        elif health_violations > 0:
            fallback_summary = f"📋 This water system serving {pop_text} has had {health_violations} health-based violation(s) in the past 2 years, but they appear to be resolved. Monitor for updates."
        elif total_violations > 0:
            fallback_summary = f"✅ This water system serving {pop_text} has good health compliance but has {total_violations} non-health monitoring/reporting violation(s) in the past 2 years."
        else:
            fallback_summary = f"✅ **GOOD NEWS**: This water system serving {pop_text} has no violations in the past 2 years and appears to be operating safely."
        
        # Add test results info if available
        if not test_results_df.empty:
            unique_contaminants = len(test_results_df['CONTAMINANT_CODE'].unique())
            fallback_summary += f" Recent testing covers {unique_contaminants} different contaminant(s)."
        
        # If Azure OpenAI is not available, return fallback summary
        if not self.azure_client:
            print("Azure OpenAI not available, returning fallback summary.")
            return fallback_summary
        
        print("Generating summary...")
        try:
            # Prepare data for AI analysis
            violations_summary = []
            if not violations_df.empty:
                for _, violation in violations_df.iterrows():
                    violations_summary.append({
                        'type': violation['VIOLATION_CATEGORY_CODE'],
                        'contaminant': violation['CONTAMINANT_CODE'],
                        'health_based': violation['IS_HEALTH_BASED_IND'] == 'Y',
                        'status': violation.get('VIOLATION_STATUS', 'Unknown'),
                        'start_date': violation.get('NON_COMPL_PER_BEGIN_DATE'),
                        'end_date': violation.get('NON_COMPL_PER_END_DATE')
                    })
            
            test_summary = []
            if not test_results_df.empty:
                for _, test in test_results_df.iterrows():
                    test_summary.append({
                        'contaminant': test['CONTAMINANT_CODE'],
                        'result': test['SAMPLE_MEASURE'],
                        'unit': test.get('UNIT_OF_MEASURE', ''),
                        'date': test['SAMPLING_END_DATE']
                    })
            
            # Create the AI prompt
            prompt = f"""You are a water safety expert analyzing data for a public water system. Generate a clear, informative summary that the general public can understand.

Water System Information:
- Population served: {pop_text}
- Total violations (last 2 years): {total_violations}
- Health-based violations: {health_violations}
- Unresolved health violations: {unresolved_health}

Violations Details: {violations_summary[:5]}  # Limit to first 5 for context
Test Results: {test_summary[:5]}  # Limit to first 5 for context

Requirements:
1. Start with an appropriate emoji and status (⚠️ for urgent attention, 📋 for monitoring needed, ✅ for good status)
2. Write in plain language that non-experts can understand
3. Be factual and balanced - neither alarmist nor dismissive  
4. Include specific numbers when relevant
5. Keep it to 2-3 sentences maximum
6. Focus on what matters most to public safety

Generate a summary suitable for display to residents served by this water system."""

            # Get deployment name from configuration
            deployment_name = os.getenv('AZURE_OPENAI_DEPLOYMENT_NAME', 'gpt-4')
            
            # Make the API call
            response = self.azure_client.chat.completions.create(
                model=deployment_name,
                messages=[
                    {"role": "system", "content": "You are a water safety expert who explains technical information clearly to the general public."},
                    {"role": "user", "content": prompt}
                ],
                max_completion_tokens=2000,
                # temperature=0.3
            )
            
            ai_summary = response.choices[0].message.content.strip()
            
            # Validate the response is reasonable
            if ai_summary and len(ai_summary) > 20:
                return ai_summary
            else:
                return fallback_summary
                
        except Exception as e:
            st.warning(f"⚠️ AI summary generation failed: {str(e)}. Using default summary.")
            return fallback_summary

class UIComponents:
    """UI component handlers"""
    
    @staticmethod
    def show_system_search(explorer):
        """Display system search interface"""
        st.subheader("🔍 Find Your Water System")
        
        # Initialize session state for selected system
        if 'selected_system' not in st.session_state:
            st.session_state.selected_system = None
        if 'search_results' not in st.session_state:
            st.session_state.search_results = pd.DataFrame()
        
        # Show safety report if a system is selected
        if st.session_state.selected_system:
            SafetyReportGenerator.show_safety_report(
                explorer, 
                st.session_state.selected_system['pwsid'], 
                st.session_state.selected_system['name'],
            )
            
            # Add button to clear the report
            st.divider()
            if st.button("🔙 Back to Search", type="secondary"):
                st.session_state.selected_system = None
                st.session_state.search_results = pd.DataFrame()
                st.rerun()
            return
        
        # Search interface
        col1, col2 = st.columns([3, 1])
        
        with col1:
            search_input = st.text_input(
                "Enter your question regarding water safety:",
                placeholder="e.g., Atlanta, or what is the water quality in Atlanta?",
                key="search_input"
            )
        
        with col2:
            search_button = st.button("Find My Water System", type="primary")
        
        # Handle search
        if search_button and search_input:
            with st.spinner("Processing your query..."):
                # Determine query type using AI
                prompt = f"""
                You are a water safety expert.
                You need to determine the type of the question. If the query is only about a specific location then return the query_type as "location" For the query_type is location, return the exact location as a 'query' instead of the original question. If the query is not about a specific location and requires complex analysis, then return the query_type as "query".
                
                Question: {search_input}
                
                Return the query_type and the processed query in the following format:
                {{
                    "query_type": "location" | "query",
                    "query": "processed_query_here"
                }}
                """

                if explorer.azure_client_low:
                    try:
                        response = explorer.azure_client_low.chat.completions.create(
                            model=os.getenv('AZURE_OPENAI_DEPLOYMENT_NAME_2'),
                            messages=[
                                {"role": "system", "content": "You are a water safety expert who analyzes queries and determines their type."},
                                {"role": "user", "content": prompt}
                            ],
                            max_completion_tokens=500,
                            temperature=0.1
                        )
                        
                        query_type, processed_query = _parse_response_for_query_type_and_query(response)
                        print("parsed response")
                        print(query_type)
                        print(processed_query)
                        
                        if query_type == "location":
                            # Use the processed query or fall back to original input
                            search_term = processed_query if processed_query else search_input
                            st.session_state.search_results = explorer.find_water_systems(search_term)
                            print("location query")
                            print(search_term)
                        elif query_type == "query":
                            # Generate and execute SQL query
                            sql_query = explorer.generate_sql_query(search_input)
                            if sql_query:
                                st.info(f"Generated SQL query: {sql_query[:200]}...")
                                query_results = explorer.execute_generated_query(sql_query)
                                st.session_state.search_results = query_results
                            else:
                                st.error("Failed to generate SQL query for your question.")
                                st.session_state.search_results = pd.DataFrame()
                            print("query query")
                            print(sql_query)
                            print(query_results)
                        
                    except Exception as e:
                        st.error(f"Error processing query: {str(e)}")
                        # Fall back to simple location search
                        st.session_state.search_results = explorer.find_water_systems(search_input)
                else:
                    # Fall back to simple location search when AI is not available
                    st.session_state.search_results = explorer.find_water_systems(search_input)
        
        # Display search results
        if not st.session_state.search_results.empty:
            UIComponents._show_search_results(st.session_state.search_results)
        elif search_button and search_input:
            st.warning("No results found. Try a different search term or question.")
        else:
            st.info("💡 **Tip:** Ask questions like 'What is the water quality in Atlanta?' or search for your city, county, zip code, or water system name.")
    
    @staticmethod
    def _show_search_results(systems_df):
        """Display search results"""
        st.success(f"Found {len(systems_df)} water system(s) in your area:")
        
        for idx, system in systems_df.iterrows():
            with st.expander(f"🏢 {system['PWS_NAME']} (PWSID: {system['PWSID']})"):
                UIComponents._show_system_preview(system)
                
                # Use a unique key for each button
                button_key = f"safety_{system['PWSID']}"
                if st.button(f"📊 View Safety Report", key=button_key, type="primary"):
                    # Store selected system in session state
                    st.session_state.selected_system = {
                        'pwsid': system['PWSID'],
                        'name': system['PWS_NAME']
                    }
                    st.rerun()
    
    @staticmethod
    def _show_system_preview(system):
        """Show system preview information"""
        col1, col2 = st.columns(2)
        
        with col1:
            st.write(f"**Type:** {DataFormatters.get_system_type_description(system['PWS_TYPE_CODE'])}")
            st.write(f"**Serves:** {system['POPULATION_SERVED_COUNT']:,.0f} people")
        
        with col2:
            st.write(f"**Location:** {system['CITY_NAME']}")
            if pd.notna(system.get('COUNTY_SERVED')):
                st.write(f"**County:** {system['COUNTY_SERVED']}")

class SafetyReportGenerator:
    """Generates water safety reports"""
    
    @staticmethod
    def show_safety_report(explorer, pwsid, system_name, summary_txt=None):
        """Display comprehensive safety report"""
        st.title(f"🛡️ Water Safety Report")
        st.subheader(f"{system_name}")
        st.write(f"**System ID (PWSID):** {pwsid}")
        st.divider()
        
        # Add a loading spinner while fetching data
        with st.spinner("Loading safety data..."):
            safety_data = explorer.get_system_safety_data(pwsid)
        
        if safety_data['basic_info'].empty:
            st.error("❌ System information not found in database.")
            SafetyReportGenerator._show_error_guidance()
            return
        
        system_info = safety_data['basic_info'].iloc[0]
        
        # Generate and show summary section
        if summary_txt is None:
            summary_txt = explorer._generate_summary(safety_data, system_info)
        
        SafetyReportGenerator._show_summary_section(summary_txt, safety_data, system_info)
        
        # Show safety status
        SafetyReportGenerator._show_safety_status(safety_data['recent_violations'])
        
        # Show system information
        SafetyReportGenerator._show_system_info(system_info, safety_data['recent_violations'])
        
        # Show test results
        SafetyReportGenerator._show_test_results(explorer, safety_data['test_results'])
        
        # Show violations
        SafetyReportGenerator._show_violations(explorer, safety_data['recent_violations'])
        
        # Show recommendations
        SafetyReportGenerator._show_recommendations(safety_data['recent_violations'], system_info)
    
    @staticmethod
    def _show_safety_status(violations_df):
        """Show overall safety status"""
        st.subheader("🏥 Safety Status")
        
        health_violations = violations_df[violations_df['IS_HEALTH_BASED_IND'] == 'Y'] if not violations_df.empty else pd.DataFrame()
        
        if health_violations.empty:
            st.success("✅ No recent health-based violations found (last 2 years)")
        else:
            unresolved = health_violations[health_violations['VIOLATION_STATUS'].isin(['Unaddressed', 'Addressed'])]
            if not unresolved.empty:
                st.error("⚠️ Active health-based violations found - Contact your water system immediately")
            else:
                st.warning("⚡ Recent health-based violations found (resolved)")
    
    @staticmethod
    def _show_system_info(system_info, violations_df):
        """Show system information"""
        st.subheader("🏢 System Information")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Basic Details:**")
            st.write(f"• **Name:** {system_info.get('PWS_NAME', 'Not Available')}")
            st.write(f"• **Type:** {DataFormatters.get_system_type_description(system_info.get('PWS_TYPE_CODE', 'Unknown'))}")
            st.write(f"• **Population Served:** {system_info.get('POPULATION_SERVED_COUNT', 0):,.0f} people")
            st.write(f"• **Water Source:** {DataFormatters.get_source_description(system_info.get('PRIMARY_SOURCE_CODE', 'Unknown'))}")
            st.write(f"• **Location:** {system_info.get('CITY_NAME', 'Not Available')}")
        
        with col2:
            st.write("**Contact & Status:**")
            st.write(f"• **Phone:** {system_info.get('PHONE_NUMBER', 'Not Available')}")
            
            if not violations_df.empty:
                health_violations = len(violations_df[violations_df['IS_HEALTH_BASED_IND'] == 'Y'])
                st.write(f"• **Total violations (2 years):** {len(violations_df)}")
                st.write(f"• **Health-based violations:** {health_violations}")
            else:
                st.write("• **Violations:** No recent violations found")
    
    @staticmethod
    def _show_test_results(explorer, test_results_df):
        """Show latest test results"""
        if test_results_df.empty:
            st.info("🧪 No recent test results available in database")
            return
        
        st.subheader("🧪 Latest Test Results")
        
        contaminants = test_results_df['CONTAMINANT_CODE'].unique()
        
        for contaminant in contaminants[:5]:
            contaminant_tests = test_results_df[test_results_df['CONTAMINANT_CODE'] == contaminant].head(3)
            
            with st.expander(f"🔬 {contaminant} Test Results"):
                for idx, test in contaminant_tests.iterrows():
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write(f"**Date:** {test['SAMPLING_END_DATE']}")
                        st.write(f"**Result:** {test.get('RESULT_SIGN_CODE', '')}{test['SAMPLE_MEASURE']} {test.get('UNIT_OF_MEASURE', '')}")
                    
                    with col2:
                        contaminant_info = explorer.health_info.get(contaminant)
                        if contaminant_info:
                            st.write(f"**Action Level:** {contaminant_info['action_level']}")
                            st.write(f"**Health Risk:** {contaminant_info['severity']}")
    
    @staticmethod
    def _show_violations(explorer, violations_df):
        """Show violations details"""
        if violations_df.empty:
            st.success("🎉 No violations found in the last 2 years!")
            return
        
        st.subheader("⚠️ Violations & Issues (Last 2 Years)")
        
        # Health-based violations
        health_violations = violations_df[violations_df['IS_HEALTH_BASED_IND'] == 'Y']
        if not health_violations.empty:
            SafetyReportGenerator._show_health_violations(explorer, health_violations)
        
        # Other violations
        other_violations = violations_df[violations_df['IS_HEALTH_BASED_IND'] != 'Y']
        if not other_violations.empty:
            SafetyReportGenerator._show_other_violations(explorer, other_violations)
    
    @staticmethod
    def _show_health_violations(explorer, health_violations):
        """Show health-based violations"""
        st.error("🚨 Health-Based Violations (Immediate Attention Required)")
        
        for idx, violation in health_violations.iterrows():
            status = violation.get('VIOLATION_STATUS', 'Unknown Status')
            with st.expander(f"🔴 {violation['VIOLATION_CATEGORY_CODE']} - {violation['CONTAMINANT_CODE']} ({status})", expanded=True):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write(f"**Started:** {violation.get('NON_COMPL_PER_BEGIN_DATE', 'Unknown')}")
                    if violation.get('NON_COMPL_PER_END_DATE'):
                        st.write(f"**Resolved:** {violation['NON_COMPL_PER_END_DATE']}")
                    st.write(f"**Status:** {status}")
                    st.write(f"**Violation Type:** {violation.get('VIOLATION_CATEGORY_CODE', 'Unknown')}")
                
                with col2:
                    contaminant_info = explorer.health_info.get(violation['CONTAMINANT_CODE'])
                    if contaminant_info:
                        st.error(f"**Health Effects:** {contaminant_info['health_effects']}")
                        st.write(f"**Common Sources:** {contaminant_info['sources']}")
                        st.write(f"**Action Level:** {contaminant_info['action_level']}")
                        st.write(f"**Risk Level:** {contaminant_info['severity']}")
                    
                    violation_explanation = explorer.violation_explanations.get(violation['VIOLATION_CATEGORY_CODE'])
                    if violation_explanation:
                        st.info(f"**What this means:** {violation_explanation}")
    
    @staticmethod
    def _show_other_violations(explorer, other_violations):
        """Show non-health-based violations"""
        st.warning("📋 Other Violations (Monitoring & Reporting)")
        
        for idx, violation in other_violations.iterrows():
            status = violation.get('VIOLATION_STATUS', 'Unknown Status')
            with st.expander(f"🟡 {violation['VIOLATION_CATEGORY_CODE']} - {violation['CONTAMINANT_CODE']} ({status})"):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write(f"**Started:** {violation.get('NON_COMPL_PER_BEGIN_DATE', 'Unknown')}")
                    if violation.get('NON_COMPL_PER_END_DATE'):
                        st.write(f"**Resolved:** {violation['NON_COMPL_PER_END_DATE']}")
                    st.write(f"**Status:** {status}")
                
                with col2:
                    violation_explanation = explorer.violation_explanations.get(violation['VIOLATION_CATEGORY_CODE'])
                    if violation_explanation:
                        st.info(f"**What this means:** {violation_explanation}")
    
    @staticmethod
    def _show_recommendations(violations_df, system_info):
        """Show action recommendations"""
        st.subheader("📞 What You Should Do")
        
        health_violations = violations_df[violations_df['IS_HEALTH_BASED_IND'] == 'Y'] if not violations_df.empty else pd.DataFrame()
        
        if not health_violations.empty:
            st.error("**Immediate Actions Required:**")
            st.write("• Contact your water system immediately for current status")
            st.write("• Ask about corrective measures being taken")
            st.write("• Consider using bottled water until resolved")
            st.write("• Sign up for system notifications")
        else:
            st.success("**Stay Informed:**")
            st.write("• Review your annual water quality report")
            st.write("• Sign up for system notifications")
            st.write("• Contact your system with any concerns")
        
        # Emergency contacts
        st.info("**Emergency Contacts:**")
        st.write("• Georgia EPD: 1-888-373-5947")
        st.write("• EPA Safe Drinking Water Hotline: 1-800-426-4791")
        st.write(f"• Your Water System: {system_info.get('PHONE_NUMBER', 'Contact information not available')}")
    
    @staticmethod
    def _show_summary_section(summary_txt, safety_data, system_info):
        """Display the summary section"""
        st.subheader("📋 Safety Summary")
        
        # Determine the appropriate message type based on content
        if "ATTENTION REQUIRED" in summary_txt or "⚠️" in summary_txt:
            st.error(summary_txt)
        elif "📋" in summary_txt and "violation" in summary_txt.lower():
            st.warning(summary_txt)
        else:
            st.success(summary_txt)
        
        # Add quick stats
        violations_df = safety_data['recent_violations']
        if not violations_df.empty:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Violations (2 years)", len(violations_df))
            with col2:
                health_violations = len(violations_df[violations_df['IS_HEALTH_BASED_IND'] == 'Y'])
                st.metric("Health-Based Violations", health_violations)
            with col3:
                population = int(system_info.get('POPULATION_SERVED_COUNT', 0))
                st.metric("Population Served", f"{population:,}")
        else:
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Violations (2 years)", "0", delta="No violations found")
            with col2:
                population = int(system_info.get('POPULATION_SERVED_COUNT', 0))
                st.metric("Population Served", f"{population:,}")
        
        st.divider()
    
    @staticmethod
    def _show_error_guidance():
        """Show guidance when system not found"""
        st.info("This could mean:")
        st.write("• The PWSID doesn't exist in the database")
        st.write("• The system is inactive")
        st.write("• There's a data synchronization issue")

class DataFormatters:
    """Data formatting utilities"""
    
    @staticmethod
    def get_system_type_description(code):
        """Get human-readable system type description"""
        descriptions = {
            'CWS': 'Community Water System (serves residents year-round)',
            'TNCWS': 'Transient Non-Community (serves travelers/visitors)',
            'NTNCWS': 'Non-Transient Non-Community (serves workers/students)'
        }
        return descriptions.get(code, code)
    
    @staticmethod
    def get_source_description(code):
        """Get human-readable water source description"""
        descriptions = {
            'GW': 'Groundwater (wells, springs)',
            'SW': 'Surface Water (rivers, lakes)',
            'GWP': 'Purchased Groundwater',
            'SWP': 'Purchased Surface Water',
            'GU': 'Groundwater Under Surface Water Influence',
            'GUP': 'Purchased Groundwater Under Surface Water Influence'
        }
        return descriptions.get(code, code)

def _parse_response_for_query_type_and_query(response):
    """Parse Azure OpenAI response to extract query_type and query"""
    try:
        content = response.choices[0].message.content.strip()
        
        # Try to parse as JSON
        try:
            data = json.loads(content)
            return data.get('query_type', 'location'), data.get('query', '')
        except json.JSONDecodeError:
            # If not valid JSON, try to extract from text
            lines = content.split('\n')
            query_type = 'location'
            query = ''
            
            for line in lines:
                if 'query_type' in line.lower():
                    if 'query' in line.lower() and 'location' not in line.lower():
                        query_type = 'query'
                    else:
                        query_type = 'location'
                elif 'query' in line.lower() and '"' in line:
                    # Extract query from quoted text
                    parts = line.split('"')
                    if len(parts) >= 2:
                        query = parts[1]
            
            return query_type, query
            
    except Exception as e:
        st.warning(f"Failed to parse AI response: {str(e)}")
        return 'location', ''

def main():
    """Main application entry point"""
    st.title("💧 Georgia Water Safety Explorer")
    st.markdown("### Find information about your local water system and understand your water quality")
    
    # Initialize the explorer
    explorer = WaterSystemExplorer()
    
    # Show the search interface
    UIComponents.show_system_search(explorer)

if __name__ == "__main__":
    main()
