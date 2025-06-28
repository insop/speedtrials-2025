# app.py
import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import folium
from streamlit_folium import st_folium
from datetime import datetime, timedelta
import numpy as np
import re

# Page configuration
st.set_page_config(
    page_title="Georgia Water Safety Explorer",
    page_icon="💧",
    layout="wide",
    initial_sidebar_state="expanded"
)

class SDWISExplorer:
    def __init__(self, db_path="sdwis_georgia.db"):
        self.db_path = db_path
    
    def get_connection(self):
        return sqlite3.connect(self.db_path)
    
    def execute_query(self, query, params=None):
        """Execute a SQL query and return results as DataFrame"""
        try:
            with self.get_connection() as conn:
                return pd.read_sql_query(query, conn, params=params or {})
        except Exception as e:
            st.error(f"Database query error: {str(e)}")
            return pd.DataFrame()
    
    # Health information mappings
    def get_contaminant_health_info(self):
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
    
    def get_violation_explanations(self):
        return {
            'MCL': 'Maximum Contaminant Level - The highest level of a contaminant allowed in drinking water',
            'MRDL': 'Maximum Residual Disinfectant Level - The highest level of disinfectant allowed',
            'TT': 'Treatment Technique - Required processes to reduce contaminant levels',
            'MR': 'Monitoring and Reporting - Required testing and reporting to ensure safety',
            'MON': 'Monitoring - Required testing was not completed',
            'RPT': 'Reporting - Required reports were not submitted'
        }

    # Public-facing queries
    def find_my_water_system(self, address_input):
        """Find water system by address, city, or zip code"""
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
        return self.execute_query(query, {'search': f'%{address_input}%'})
    
    def get_system_safety_summary(self, pwsid):
        """Get safety summary for public"""
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
            'latest_test_results': """
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

    # Operator-facing queries
    def get_operator_dashboard(self, pwsid):
        """Get operator dashboard data"""
        queries = {
            'system_info': """
                SELECT * FROM pub_water_systems WHERE PWSID = :pwsid
            """,
            'active_violations': """
                SELECT * FROM violations_enforcement 
                WHERE PWSID = :pwsid AND VIOLATION_STATUS IN ('Unaddressed', 'Addressed')
                ORDER BY NON_COMPL_PER_BEGIN_DATE DESC
            """,
            'upcoming_requirements': """
                SELECT * FROM events_milestones 
                WHERE PWSID = :pwsid 
                AND EVENT_END_DATE >= date('now')
                ORDER BY EVENT_END_DATE ASC
            """,
            'recent_inspections': """
                SELECT * FROM site_visits 
                WHERE PWSID = :pwsid 
                ORDER BY VISIT_DATE DESC
                LIMIT 5
            """,
            'facilities': """
                SELECT * FROM facilities 
                WHERE PWSID = :pwsid AND FACILITY_ACTIVITY_CODE = 'A'
            """
        }
        
        results = {}
        for key, query in queries.items():
            results[key] = self.execute_query(query, {'pwsid': pwsid})
        return results
    
    def get_compliance_calendar(self, pwsid):
        """Get compliance calendar for operators"""
        query = """
        SELECT EVENT_MILESTONE_CODE, EVENT_END_DATE, EVENT_COMMENTS_TEXT,
               EVENT_REASON_CODE, EVENT_ACTUAL_DATE
        FROM events_milestones 
        WHERE PWSID = :pwsid 
        AND EVENT_END_DATE >= date('now', '-6 months')
        ORDER BY EVENT_END_DATE ASC
        """
        return self.execute_query(query, {'pwsid': pwsid})

    # Regulator-facing queries
    def get_regulator_field_kit(self, pwsid):
        """Get quick field reference for regulators"""
        queries = {
            'system_snapshot': """
                SELECT p.PWSID, p.PWS_NAME, p.PWS_TYPE_CODE, p.POPULATION_SERVED_COUNT,
                       p.PRIMARY_SOURCE_CODE, p.OWNER_TYPE_CODE, p.SERVICE_CONNECTIONS_COUNT,
                       p.CITY_NAME, p.PHONE_NUMBER, p.ADMIN_NAME
                FROM pub_water_systems p WHERE p.PWSID = :pwsid
            """,
            'violation_summary': """
                SELECT VIOLATION_CATEGORY_CODE, COUNT(*) as count,
                       SUM(CASE WHEN IS_HEALTH_BASED_IND = 'Y' THEN 1 ELSE 0 END) as health_based_count,
                       MAX(NON_COMPL_PER_BEGIN_DATE) as latest_violation
                FROM violations_enforcement 
                WHERE PWSID = :pwsid 
                GROUP BY VIOLATION_CATEGORY_CODE
            """,
            'enforcement_history': """
                SELECT ENFORCEMENT_ACTION_TYPE_CODE, ENFORCEMENT_DATE, 
                       ENF_ACTION_CATEGORY
                FROM violations_enforcement 
                WHERE PWSID = :pwsid AND ENFORCEMENT_DATE IS NOT NULL
                ORDER BY ENFORCEMENT_DATE DESC
                LIMIT 10
            """,
            'inspection_history': """
                SELECT VISIT_DATE, VISIT_REASON_CODE, COMPLIANCE_EVAL_CODE,
                       TREATMENT_EVAL_CODE, DISTRIBUTION_EVAL_CODE
                FROM site_visits 
                WHERE PWSID = :pwsid 
                ORDER BY VISIT_DATE DESC
                LIMIT 5
            """
        }
        
        results = {}
        for key, query in queries.items():
            results[key] = self.execute_query(query, {'pwsid': pwsid})
        return results
    
    def get_regional_overview(self, county=None):
        """Get regional overview for regulators"""
        base_query = """
        SELECT p.PWSID, p.PWS_NAME, p.PWS_TYPE_CODE, p.POPULATION_SERVED_COUNT,
               g.COUNTY_SERVED, 
               COUNT(v.VIOLATION_ID) as total_violations,
               SUM(CASE WHEN v.IS_HEALTH_BASED_IND = 'Y' THEN 1 ELSE 0 END) as health_violations,
               MAX(sv.VISIT_DATE) as last_inspection
        FROM pub_water_systems p
        LEFT JOIN geographic_areas g ON p.PWSID = g.PWSID
        LEFT JOIN violations_enforcement v ON p.PWSID = v.PWSID
        LEFT JOIN site_visits sv ON p.PWSID = sv.PWSID
        WHERE p.PWS_ACTIVITY_CODE = 'A'
        """
        
        if county:
            base_query += " AND UPPER(g.COUNTY_SERVED) = UPPER(:county)"
            params = {'county': county}
        else:
            params = {}
        
        base_query += """
        GROUP BY p.PWSID, p.PWS_NAME, p.PWS_TYPE_CODE, p.POPULATION_SERVED_COUNT, g.COUNTY_SERVED
        ORDER BY total_violations DESC, p.POPULATION_SERVED_COUNT DESC
        """
        
        return self.execute_query(base_query, params)

def main():
    st.title("🏛️ Georgia Water Safety Explorer")
    
    # Initialize the explorer
    explorer = SDWISExplorer()
    
    # Stakeholder selection
    st.sidebar.title("👥 I am a...")
    stakeholder = st.sidebar.selectbox(
        "Select your role:",
        ["Georgia Resident (Public)", "Water System Operator", "Regulator/Inspector"]
    )
    
    if stakeholder == "Georgia Resident (Public)":
        show_public_interface(explorer)
    elif stakeholder == "Water System Operator":
        show_operator_interface(explorer)
    elif stakeholder == "Regulator/Inspector":
        show_regulator_interface(explorer)

def show_public_interface(explorer):
    st.header("💧 Is My Water Safe?")
    st.markdown("### Find information about your local water system and understand your water quality")
    
    # Initialize session state for safety report display
    if 'show_safety_report' not in st.session_state:
        st.session_state.show_safety_report = False
    if 'selected_pwsid' not in st.session_state:
        st.session_state.selected_pwsid = None
    if 'selected_system_name' not in st.session_state:
        st.session_state.selected_system_name = None
    
    # Check if we should show a safety report
    if st.session_state.show_safety_report and st.session_state.selected_pwsid:
        # Display safety report
        col1, col2 = st.columns([1, 4])
        with col1:
            if st.button("← Back to Search", type="secondary"):
                st.session_state.show_safety_report = False
                st.session_state.selected_pwsid = None
                st.session_state.selected_system_name = None
                st.rerun()
        
        show_water_safety_report(explorer, st.session_state.selected_pwsid, st.session_state.selected_system_name)
        return  # Don't show the rest of the interface when displaying safety report
    
    # Water system finder
    st.subheader("🔍 Find Your Water System")
    col1, col2 = st.columns([3, 1])
    
    with col1:
        search_input = st.text_input(
            "Enter your city, county, zip code, or water system name:",
            placeholder="e.g., Atlanta, Fulton County, 30309, or City of Atlanta"
        )
    
    with col2:
        search_button = st.button("Find My Water System", type="primary")
    
    if search_button and search_input:
        systems = explorer.find_my_water_system(search_input)
        
        if not systems.empty:
            st.success(f"Found {len(systems)} water system(s) in your area:")
            
            for idx, system in systems.iterrows():
                with st.expander(f"🏢 {system['PWS_NAME']} (PWSID: {system['PWSID']})"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write(f"**Type:** {get_system_type_description(system['PWS_TYPE_CODE'])}")
                        st.write(f"**Serves:** {system['POPULATION_SERVED_COUNT']:,.0f} people")
                        st.write(f"**Location:** {system['CITY_NAME']}, {system['COUNTY_SERVED']} County")
                    
                    with col2:
                        if st.button(f"View Safety Report", key=f"safety_{system['PWSID']}"):
                            st.session_state.show_safety_report = True
                            st.session_state.selected_pwsid = system['PWSID']
                            st.session_state.selected_system_name = system['PWS_NAME']
                            st.experimental_rerun()
        else:
            st.warning("No water systems found. Try a different search term.")
    
    # Educational content
    st.subheader("📚 Understanding Your Water Quality")
    
    tab1, tab2, tab3 = st.tabs(["Common Contaminants", "Violation Types", "What You Can Do"])
    
    with tab1:
        show_contaminant_education(explorer)
    
    with tab2:
        show_violation_education(explorer)
    
    with tab3:
        show_action_guidance()

def show_water_safety_report(explorer, pwsid, system_name):
    st.subheader(f"🛡️ Water Safety Report: {system_name}")
    
    safety_data = explorer.get_system_safety_summary(pwsid)
    
    if safety_data['basic_info'].empty:
        st.error("System information not found.")
        return
    
    system_info = safety_data['basic_info'].iloc[0]
    
    # Safety status indicator
    recent_violations = safety_data['recent_violations']
    health_violations = recent_violations[recent_violations['IS_HEALTH_BASED_IND'] == 'Y'] if not recent_violations.empty else pd.DataFrame()
    
    if health_violations.empty:
        st.success("✅ No recent health-based violations found")
        safety_color = "green"
    else:
        unresolved_health = health_violations[health_violations['VIOLATION_STATUS'].isin(['Unaddressed', 'Addressed'])]
        if not unresolved_health.empty:
            st.error("⚠️ Active health-based violations found")
            safety_color = "red"
        else:
            st.warning("⚡ Recent health-based violations (resolved)")
            safety_color = "orange"
    
    # System details
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**System Information:**")
        st.write(f"• Type: {get_system_type_description(system_info['PWS_TYPE_CODE'])}")
        st.write(f"• Population Served: {system_info['POPULATION_SERVED_COUNT']:,.0f}")
        st.write(f"• Water Source: {get_source_description(system_info['PRIMARY_SOURCE_CODE'])}")
        st.write(f"• Contact: {system_info['PHONE_NUMBER']}")
    
    with col2:
        st.write("**Recent Activity:**")
        if not recent_violations.empty:
            st.write(f"• Total violations (2 years): {len(recent_violations)}")
            st.write(f"• Health-based violations: {len(health_violations)}")
        else:
            st.write("• No recent violations")
    
    # Recent violations details
    if not recent_violations.empty:
        st.subheader("Recent Violations (Last 2 Years)")
        
        for idx, violation in recent_violations.iterrows():
            severity = "🔴" if violation['IS_HEALTH_BASED_IND'] == 'Y' else "🟡"
            status_color = "red" if violation['VIOLATION_STATUS'] in ['Unaddressed', 'Addressed'] else "green"
            
            with st.expander(f"{severity} {violation['VIOLATION_CATEGORY_CODE']} - {violation['CONTAMINANT_CODE']} ({violation['VIOLATION_STATUS']})"):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write(f"**Started:** {violation['NON_COMPL_PER_BEGIN_DATE']}")
                    if violation['NON_COMPL_PER_END_DATE']:
                        st.write(f"**Resolved:** {violation['NON_COMPL_PER_END_DATE']}")
                    st.write(f"**Status:** {violation['VIOLATION_STATUS']}")
                
                with col2:
                    # Show health information if available
                    contaminant_info = explorer.get_contaminant_health_info().get(violation['CONTAMINANT_CODE'])
                    if contaminant_info:
                        st.write(f"**Health Effects:** {contaminant_info['health_effects']}")
                        st.write(f"**Severity:** {contaminant_info['severity']}")

def show_operator_interface(explorer):
    st.header("🔧 Water System Operator Dashboard")
    st.markdown("### Manage your water system compliance and operations")
    
    # System selection
    pwsid = st.text_input("Enter your PWSID:", placeholder="e.g., GA0000001")
    
    if pwsid:
        operator_data = explorer.get_operator_dashboard(pwsid)
        
        if operator_data['system_info'].empty:
            st.error("System not found. Please check your PWSID.")
            return
        
        system_info = operator_data['system_info'].iloc[0]
        st.success(f"Welcome, {system_info['PWS_NAME']} operator!")
        
        # Dashboard tabs
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "🏠 Overview", "⚠️ Active Issues", "📅 Compliance Calendar", 
            "🏭 Facilities", "📊 Reports"
        ])
        
        with tab1:
            show_operator_overview(operator_data, system_info)
        
        with tab2:
            show_operator_violations(operator_data)
        
        with tab3:
            show_compliance_calendar(explorer, pwsid)
        
        with tab4:
            show_operator_facilities(operator_data)
        
        with tab5:
            show_operator_reports(operator_data, system_info)

def show_operator_overview(operator_data, system_info):
    st.subheader("System Overview")
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    active_violations = len(operator_data['active_violations'])
    health_violations = len(operator_data['active_violations'][
        operator_data['active_violations']['IS_HEALTH_BASED_IND'] == 'Y'
    ]) if not operator_data['active_violations'].empty else 0
    
    with col1:
        st.metric("Population Served", f"{system_info['POPULATION_SERVED_COUNT']:,.0f}")
    
    with col2:
        st.metric("Service Connections", f"{system_info['SERVICE_CONNECTIONS_COUNT']:,.0f}")
    
    with col3:
        color = "red" if active_violations > 0 else "green"
        st.metric("Active Violations", active_violations)
    
    with col4:
        color = "red" if health_violations > 0 else "green"
        st.metric("Health-Based Violations", health_violations)
    
    # System details
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("System Information")
        st.write(f"**PWSID:** {system_info['PWSID']}")
        st.write(f"**Type:** {get_system_type_description(system_info['PWS_TYPE_CODE'])}")
        st.write(f"**Primary Source:** {get_source_description(system_info['PRIMARY_SOURCE_CODE'])}")
        st.write(f"**Owner Type:** {system_info['OWNER_TYPE_CODE']}")
        st.write(f"**Status:** {system_info['PWS_ACTIVITY_CODE']}")
    
    with col2:
        st.subheader("Contact Information")
        st.write(f"**Admin Contact:** {system_info['ADMIN_NAME']}")
        st.write(f"**Phone:** {system_info['PHONE_NUMBER']}")
        st.write(f"**Email:** {system_info['EMAIL_ADDR']}")
        st.write(f"**Address:** {system_info['ADDRESS_LINE1']}")
        st.write(f"**City:** {system_info['CITY_NAME']}, {system_info['STATE_CODE']} {system_info['ZIP_CODE']}")

def show_operator_violations(operator_data):
    st.subheader("Active Violations & Issues")
    
    active_violations = operator_data['active_violations']
    
    if active_violations.empty:
        st.success("🎉 No active violations! Your system is in compliance.")
        return
    
    # Priority violations (health-based)
    health_violations = active_violations[active_violations['IS_HEALTH_BASED_IND'] == 'Y']
    
    if not health_violations.empty:
        st.error("🚨 PRIORITY: Health-Based Violations Require Immediate Attention")
        
        for idx, violation in health_violations.iterrows():
            with st.expander(f"🔴 {violation['VIOLATION_CATEGORY_CODE']} - {violation['CONTAMINANT_CODE']}", expanded=True):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write(f"**Violation ID:** {violation['VIOLATION_ID']}")
                    st.write(f"**Started:** {violation['NON_COMPL_PER_BEGIN_DATE']}")
                    st.write(f"**Status:** {violation['VIOLATION_STATUS']}")
                    
                with col2:
                    st.write("**Required Actions:**")
                    st.write("• Contact your primacy agency immediately")
                    st.write("• Issue public notification if required")
                    st.write("• Implement corrective measures")
                    
                    if violation['PUBLIC_NOTIFICATION_TIER']:
                        st.write(f"**Public Notice Tier:** {violation['PUBLIC_NOTIFICATION_TIER']}")
    
    # Other violations
    other_violations = active_violations[active_violations['IS_HEALTH_BASED_IND'] != 'Y']
    
    if not other_violations.empty:
        st.warning("📋 Other Active Violations")
        
        for idx, violation in other_violations.iterrows():
            with st.expander(f"🟡 {violation['VIOLATION_CATEGORY_CODE']} - {violation['CONTAMINANT_CODE']}"):
                st.write(f"**Started:** {violation['NON_COMPL_PER_BEGIN_DATE']}")
                st.write(f"**Status:** {violation['VIOLATION_STATUS']}")
                st.write(f"**Category:** {violation['VIOLATION_CATEGORY_CODE']}")

def show_compliance_calendar(explorer, pwsid):
    st.subheader("Compliance Calendar")
    
    calendar_data = explorer.get_compliance_calendar(pwsid)
    
    if calendar_data.empty:
        st.info("No upcoming compliance requirements found.")
        return
    
    # Upcoming deadlines
    upcoming = calendar_data[calendar_data['EVENT_END_DATE'] >= datetime.now().strftime('%Y-%m-%d')]
    
    if not upcoming.empty:
        st.warning("⏰ Upcoming Deadlines")
        
        for idx, event in upcoming.iterrows():
            days_until = (pd.to_datetime(event['EVENT_END_DATE']) - datetime.now()).days
            
            if days_until <= 30:
                urgency = "🔴" if days_until <= 7 else "🟡"
            else:
                urgency = "🟢"
            
            with st.expander(f"{urgency} {event['EVENT_MILESTONE_CODE']} - Due: {event['EVENT_END_DATE']} ({days_until} days)"):
                st.write(f"**Event:** {event['EVENT_MILESTONE_CODE']}")
                st.write(f"**Reason:** {event['EVENT_REASON_CODE']}")
                if event['EVENT_COMMENTS_TEXT']:
                    st.write(f"**Details:** {event['EVENT_COMMENTS_TEXT']}")
                
                if event['EVENT_ACTUAL_DATE']:
                    st.success(f"✅ Completed on: {event['EVENT_ACTUAL_DATE']}")

def show_operator_facilities(operator_data):
    st.subheader("System Facilities")
    
    facilities = operator_data['facilities']
    
    if facilities.empty:
        st.info("No facility information available.")
        return
    
    # Group by facility type
    facility_types = facilities['FACILITY_TYPE_CODE'].unique()
    
    for facility_type in facility_types:
        type_facilities = facilities[facilities['FACILITY_TYPE_CODE'] == facility_type]
        
        with st.expander(f"🏭 {facility_type} Facilities ({len(type_facilities)})"):
            for idx, facility in type_facilities.iterrows():
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write(f"**Name:** {facility['FACILITY_NAME']}")
                    st.write(f"**ID:** {facility['FACILITY_ID']}")
                    st.write(f"**Status:** {facility['FACILITY_ACTIVITY_CODE']}")
                
                with col2:
                    st.write(f"**Type:** {facility['FACILITY_TYPE_CODE']}")
                    if facility['WATER_TYPE_CODE']:
                        st.write(f"**Water Type:** {facility['WATER_TYPE_CODE']}")
                    if facility['IS_SOURCE_IND'] == 'Y':
                        st.write("**Source Facility:** Yes")

def show_operator_reports(operator_data, system_info):
    st.subheader("System Reports")
    
    # Generate compliance summary
    active_violations = operator_data['active_violations']
    recent_inspections = operator_data['recent_inspections']
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Compliance Summary:**")
        if active_violations.empty:
            st.success("✅ System is in full compliance")
        else:
            st.error(f"❌ {len(active_violations)} active violations")
            
            violation_types = active_violations['VIOLATION_CATEGORY_CODE'].value_counts()
            for vtype, count in violation_types.items():
                st.write(f"• {vtype}: {count}")
    
    with col2:
        st.write("**Recent Inspection Summary:**")
        if not recent_inspections.empty:
            latest_inspection = recent_inspections.iloc[0]
            st.write(f"**Last Inspection:** {latest_inspection['VISIT_DATE']}")
            st.write(f"**Reason:** {latest_inspection['VISIT_REASON_CODE']}")
            st.write(f"**Compliance Result:** {latest_inspection['COMPLIANCE_EVAL_CODE']}")
        else:
            st.info("No recent inspections found")

def show_regulator_interface(explorer):
    st.header("🏛️ Regulator Field Kit")
    st.markdown("### Quick access to system information for inspections and oversight")
    
    # Quick system lookup
    col1, col2 = st.columns([3, 1])
    
    with col1:
        pwsid = st.text_input("Enter PWSID for field inspection:", placeholder="GA0000001")
    
    with col2:
        if st.button("Load System", type="primary"):
            if pwsid:
                show_regulator_field_kit(explorer, pwsid)
    
    # Regional overview
    st.subheader("🗺️ Regional Overview")
    
    col1, col2 = st.columns(2)
    
    with col1:
        county = st.selectbox("Select County:", ["All Counties"] + get_georgia_counties())
    
    with col2:
        if st.button("Load Regional Data"):
            county_filter = None if county == "All Counties" else county
            show_regional_overview(explorer, county_filter)

def show_regulator_field_kit(explorer, pwsid):
    st.subheader(f"🔍 Field Kit: {pwsid}")
    
    field_data = explorer.get_regulator_field_kit(pwsid)
    
    if field_data['system_snapshot'].empty:
        st.error("System not found.")
        return
    
    system = field_data['system_snapshot'].iloc[0]
    
    # Quick status indicators
    col1, col2, col3, col4 = st.columns(4)
    
    violation_summary = field_data['violation_summary']
    total_violations = violation_summary['count'].sum() if not violation_summary.empty else 0
    health_violations = violation_summary['health_based_count'].sum() if not violation_summary.empty else 0
    
    with col1:
        st.metric("Population", f"{system['POPULATION_SERVED_COUNT']:,.0f}")
    
    with col2:
        color = "red" if total_violations > 0 else "green"
        st.metric("Total Violations", total_violations)
    
    with col3:
        color = "red" if health_violations > 0 else "green"
        st.metric("Health Violations", health_violations)
    
    with col4:
        recent_inspections = field_data['inspection_history']
        last_inspection = recent_inspections.iloc[0]['VISIT_DATE'] if not recent_inspections.empty else "None"
        st.metric("Last Inspection", last_inspection)
    
    # System snapshot
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("System Snapshot")
        st.write(f"**Name:** {system['PWS_NAME']}")
        st.write(f"**Type:** {get_system_type_description(system['PWS_TYPE_CODE'])}")
        st.write(f"**Source:** {get_source_description(system['PRIMARY_SOURCE_CODE'])}")
        st.write(f"**Owner:** {system['OWNER_TYPE_CODE']}")
        st.write(f"**Connections:** {system['SERVICE_CONNECTIONS_COUNT']:,.0f}")
        
        st.subheader("Contact")
        st.write(f"**Admin:** {system['ADMIN_NAME']}")
        st.write(f"**Phone:** {system['PHONE_NUMBER']}")
        st.write(f"**Location:** {system['CITY_NAME']}")
    
    with col2:
        st.subheader("Compliance Status")
        
        if not violation_summary.empty:
            for idx, vtype in violation_summary.iterrows():
                severity = "🔴" if vtype['health_based_count'] > 0 else "🟡"
                st.write(f"{severity} **{vtype['VIOLATION_CATEGORY_CODE']}:** {vtype['count']} total, {vtype['health_based_count']} health-based")
        else:
            st.success("✅ No violations on record")
        
        st.subheader("Enforcement History")
        enforcement = field_data['enforcement_history']
        if not enforcement.empty:
            for idx, action in enforcement.head(3).iterrows():
                st.write(f"• {action['ENFORCEMENT_DATE']}: {action['ENFORCEMENT_ACTION_TYPE_CODE']}")
        else:
            st.info("No enforcement actions")
    
    # Detailed tabs for field reference
    tab1, tab2, tab3 = st.tabs(["📋 Violations Detail", "🔍 Inspection History", "⚖️ Enforcement"])
    
    with tab1:
        if not violation_summary.empty:
            st.dataframe(violation_summary, use_container_width=True)
        else:
            st.info("No violations found")
    
    with tab2:
        inspections = field_data['inspection_history']
        if not inspections.empty:
            st.dataframe(inspections, use_container_width=True)
        else:
            st.info("No inspection history found")
    
    with tab3:
        enforcement = field_data['enforcement_history']
        if not enforcement.empty:
            st.dataframe(enforcement, use_container_width=True)
        else:
            st.info("No enforcement history found")

def show_regional_overview(explorer, county):
    st.subheader(f"Regional Overview: {county or 'All Counties'}")
    
    regional_data = explorer.get_regional_overview(county)
    
    if regional_data.empty:
        st.warning("No data found for selected region.")
        return
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    total_systems = len(regional_data)
    total_population = regional_data['POPULATION_SERVED_COUNT'].sum()
    systems_with_violations = len(regional_data[regional_data['total_violations'] > 0])
    systems_with_health_violations = len(regional_data[regional_data['health_violations'] > 0])
    
    with col1:
        st.metric("Total Systems", total_systems)
    
    with col2:
        st.metric("Population Served", f"{total_population:,.0f}")
    
    with col3:
        st.metric("Systems w/ Violations", f"{systems_with_violations} ({systems_with_violations/total_systems*100:.1f}%)")
    
    with col4:
        st.metric("Health Violations", f"{systems_with_health_violations} ({systems_with_health_violations/total_systems*100:.1f}%)")
    
    # Priority systems (those with health violations)
    priority_systems = regional_data[regional_data['health_violations'] > 0].head(10)
    
    if not priority_systems.empty:
        st.subheader("🚨 Priority Systems (Health Violations)")
        
        for idx, system in priority_systems.iterrows():
            with st.expander(f"🔴 {system['PWS_NAME']} - {system['health_violations']} health violations"):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write(f"**PWSID:** {system['PWSID']}")
                    st.write(f"**Population:** {system['POPULATION_SERVED_COUNT']:,.0f}")
                    st.write(f"**Type:** {system['PWS_TYPE_CODE']}")
                
                with col2:
                    st.write(f"**Total Violations:** {system['total_violations']}")
                    st.write(f"**Health Violations:** {system['health_violations']}")
                    st.write(f"**Last Inspection:** {system['last_inspection'] or 'None'}")
    
    # Systems overview table
    st.subheader("All Systems Overview")
    
    # Add risk scoring
    regional_data['risk_score'] = (
        regional_data['health_violations'] * 3 + 
        regional_data['total_violations'] * 1 +
        regional_data['POPULATION_SERVED_COUNT'] / 10000
    )
    
    display_data = regional_data[['PWS_NAME', 'PWS_TYPE_CODE', 'POPULATION_SERVED_COUNT', 
                                 'total_violations', 'health_violations', 'last_inspection', 'risk_score']].copy()
    
    display_data = display_data.sort_values('risk_score', ascending=False)
    
    st.dataframe(
        display_data,
        column_config={
            "PWS_NAME": "System Name",
            "PWS_TYPE_CODE": "Type",
            "POPULATION_SERVED_COUNT": st.column_config.NumberColumn("Population", format="%d"),
            "total_violations": "Total Violations",
            "health_violations": "Health Violations",
            "last_inspection": "Last Inspection",
            "risk_score": st.column_config.NumberColumn("Risk Score", format="%.1f")
        },
        use_container_width=True
    )

# Helper functions
def get_system_type_description(code):
    descriptions = {
        'CWS': 'Community Water System (serves residents year-round)',
        'TNCWS': 'Transient Non-Community (serves travelers/visitors)',
        'NTNCWS': 'Non-Transient Non-Community (serves workers/students)'
    }
    return descriptions.get(code, code)

def get_source_description(code):
    descriptions = {
        'GW': 'Groundwater (wells, springs)',
        'SW': 'Surface Water (rivers, lakes)',
        'GWP': 'Purchased Groundwater',
        'SWP': 'Purchased Surface Water',
        'GU': 'Groundwater Under Surface Water Influence',
        'GUP': 'Purchased Groundwater Under Surface Water Influence'
    }
    return descriptions.get(code, code)

def get_georgia_counties():
    # Simplified list - in production, this would come from the database
    return [
        'FULTON', 'DEKALB', 'GWINNETT', 'COBB', 'CLAYTON', 'HENRY', 'CHEROKEE',
        'FORSYTH', 'HALL', 'MUSCOGEE', 'BIBB', 'RICHMOND', 'CHATHAM', 'CLARKE'
    ]

def show_contaminant_education(explorer):
    st.write("### Common Contaminants and Health Effects")
    
    contaminant_info = explorer.get_contaminant_health_info()
    
    for contaminant, info in contaminant_info.items():
        with st.expander(f"🧪 {contaminant}"):
            col1, col2 = st.columns(2)
            
            with col1:
                st.write(f"**Health Effects:** {info['health_effects']}")
                st.write(f"**Common Sources:** {info['sources']}")
            
            with col2:
                st.write(f"**Action Level:** {info['action_level']}")
                severity_color = {"High": "🔴", "Medium": "🟡", "Low": "🟢"}
                st.write(f"**Severity:** {severity_color.get(info['severity'], '🟡')} {info['severity']}")

def show_violation_education(explorer):
    st.write("### Understanding Violation Types")
    
    violation_info = explorer.get_violation_explanations()
    
    for violation_type, explanation in violation_info.items():
        with st.expander(f"📋 {violation_type}"):
            st.write(explanation)
            
            if violation_type in ['MCL', 'MRDL', 'TT']:
                st.warning("⚠️ This is a health-based violation that requires immediate attention.")

def show_action_guidance():
    st.write("### What You Can Do")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**If You Find Violations:**")
        st.write("• Contact your water system directly")
        st.write("• Ask about corrective actions being taken")
        st.write("• Request public notifications")
        st.write("• Consider temporary alternatives if health-based")
        
        st.write("**Stay Informed:**")
        st.write("• Sign up for water system notifications")
        st.write("• Review annual water quality reports")
        st.write("• Attend public meetings")
    
    with col2:
        st.write("**Additional Resources:**")
        st.write("• [EPA Safe Drinking Water Hotline](tel:1-800-426-4791): 1-800-426-4791")
        st.write("• [Georgia EPD](https://epd.georgia.gov/)")
        st.write("• [CDC Water Quality Information](https://www.cdc.gov/healthywater/)")
        
        st.write("**Emergency Contacts:**")
        st.write("• Local Health Department")
        st.write("• Georgia Environmental Protection Division")
        st.write("• EPA Region 4: 1-800-241-1754")

if __name__ == "__main__":
    main()
