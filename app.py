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

# Page configuration
st.set_page_config(
    page_title="Georgia SDWIS Data Explorer",
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
        with self.get_connection() as conn:
            return pd.read_sql_query(query, conn, params=params or {})
    
    def get_summary_stats(self):
        """Get overall summary statistics"""
        queries = {
            'total_systems': "SELECT COUNT(DISTINCT PWSID) as count FROM pub_water_systems",
            'total_violations': "SELECT COUNT(*) as count FROM violations_enforcement",
            'active_systems': "SELECT COUNT(DISTINCT PWSID) as count FROM pub_water_systems WHERE PWS_ACTIVITY_CODE = 'A'",
            'total_population': "SELECT SUM(POPULATION_SERVED_COUNT) as total FROM pub_water_systems WHERE PWS_ACTIVITY_CODE = 'A'"
        }
        
        stats = {}
        for key, query in queries.items():
            result = self.execute_query(query)
            stats[key] = result.iloc[0, 0] if not result.empty else 0
        
        return stats
    
    def get_systems_by_type(self):
        """Get water systems breakdown by type"""
        query = """
        SELECT PWS_TYPE_CODE, COUNT(*) as count, 
               SUM(POPULATION_SERVED_COUNT) as total_population
        FROM pub_water_systems 
        WHERE PWS_ACTIVITY_CODE = 'A'
        GROUP BY PWS_TYPE_CODE
        """
        return self.execute_query(query)
    
    def get_violations_by_type(self):
        """Get violations breakdown by category"""
        query = """
        SELECT VIOLATION_CATEGORY_CODE, COUNT(*) as count
        FROM violations_enforcement
        GROUP BY VIOLATION_CATEGORY_CODE
        ORDER BY count DESC
        """
        return self.execute_query(query)
    
    def get_violations_over_time(self):
        """Get violations over time"""
        query = """
        SELECT DATE(NON_COMPL_PER_BEGIN_DATE) as violation_date, 
               COUNT(*) as count,
               VIOLATION_CATEGORY_CODE
        FROM violations_enforcement
        WHERE NON_COMPL_PER_BEGIN_DATE IS NOT NULL
        GROUP BY DATE(NON_COMPL_PER_BEGIN_DATE), VIOLATION_CATEGORY_CODE
        ORDER BY violation_date
        """
        return self.execute_query(query)
    
    def get_systems_by_county(self):
        """Get systems by county"""
        query = """
        SELECT ga.COUNTY_SERVED, COUNT(DISTINCT ga.PWSID) as system_count,
               AVG(pws.POPULATION_SERVED_COUNT) as avg_population
        FROM geographic_areas ga
        JOIN pub_water_systems pws ON ga.PWSID = pws.PWSID
        WHERE ga.COUNTY_SERVED IS NOT NULL AND pws.PWS_ACTIVITY_CODE = 'A'
        GROUP BY ga.COUNTY_SERVED
        ORDER BY system_count DESC
        """
        return self.execute_query(query)
    
    def search_systems(self, search_term="", system_type="", min_population=0, max_population=1000000):
        """Search water systems with filters"""
        query = """
        SELECT PWSID, PWS_NAME, PWS_TYPE_CODE, POPULATION_SERVED_COUNT,
               CITY_NAME, STATE_CODE, PWS_ACTIVITY_CODE
        FROM pub_water_systems
        WHERE 1=1
        """
        params = {}
        
        if search_term:
            query += " AND (PWS_NAME LIKE :search OR PWSID LIKE :search)"
            params['search'] = f"%{search_term}%"
        
        if system_type:
            query += " AND PWS_TYPE_CODE = :system_type"
            params['system_type'] = system_type
        
        query += " AND POPULATION_SERVED_COUNT BETWEEN :min_pop AND :max_pop"
        params['min_pop'] = min_population
        params['max_pop'] = max_population
        
        query += " ORDER BY POPULATION_SERVED_COUNT DESC LIMIT 100"
        
        return self.execute_query(query, params)
    
    def get_system_details(self, pwsid):
        """Get detailed information for a specific system"""
        queries = {
            'basic_info': """
                SELECT * FROM pub_water_systems WHERE PWSID = :pwsid
            """,
            'violations': """
                SELECT * FROM violations_enforcement 
                WHERE PWSID = :pwsid 
                ORDER BY NON_COMPL_PER_BEGIN_DATE DESC
            """,
            'facilities': """
                SELECT * FROM facilities WHERE PWSID = :pwsid
            """,
            'site_visits': """
                SELECT * FROM site_visits 
                WHERE PWSID = :pwsid 
                ORDER BY VISIT_DATE DESC
            """
        }
        
        details = {}
        for key, query in queries.items():
            details[key] = self.execute_query(query, {'pwsid': pwsid})
        
        return details

def main():
    st.title("🏛️ Georgia Safe Drinking Water Information System (SDWIS) Explorer")
    st.markdown("### Explore public water system data, violations, and compliance for the state of Georgia")
    
    # Initialize the explorer
    explorer = SDWISExplorer()
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox(
        "Choose a page:",
        ["Dashboard", "System Search", "Violations Analysis", "Geographic View", "System Details"]
    )
    
    if page == "Dashboard":
        show_dashboard(explorer)
    elif page == "System Search":
        show_system_search(explorer)
    elif page == "Violations Analysis":
        show_violations_analysis(explorer)
    elif page == "Geographic View":
        show_geographic_view(explorer)
    elif page == "System Details":
        show_system_details(explorer)

def show_dashboard(explorer):
    st.header("📊 Dashboard Overview")
    
    # Get summary statistics
    stats = explorer.get_summary_stats()
    
    # Display key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Water Systems", f"{stats['total_systems']:,}")
    
    with col2:
        st.metric("Active Systems", f"{stats['active_systems']:,}")
    
    with col3:
        st.metric("Total Violations", f"{stats['total_violations']:,}")
    
    with col4:
        st.metric("Population Served", f"{stats['total_population']:,.0f}")
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Systems by Type")
        systems_by_type = explorer.get_systems_by_type()
        if not systems_by_type.empty:
            fig = px.pie(systems_by_type, values='count', names='PWS_TYPE_CODE',
                        title="Distribution of Water System Types")
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Violations by Category")
        violations_by_type = explorer.get_violations_by_type()
        if not violations_by_type.empty:
            fig = px.bar(violations_by_type, x='VIOLATION_CATEGORY_CODE', y='count',
                        title="Violations by Category")
            st.plotly_chart(fig, use_container_width=True)
    
    # Violations over time
    st.subheader("Violations Over Time")
    violations_time = explorer.get_violations_over_time()
    if not violations_time.empty:
        violations_time['violation_date'] = pd.to_datetime(violations_time['violation_date'])
        fig = px.line(violations_time, x='violation_date', y='count', 
                     color='VIOLATION_CATEGORY_CODE',
                     title="Violations Over Time by Category")
        st.plotly_chart(fig, use_container_width=True)

def show_system_search(explorer):
    st.header("🔍 Water System Search")
    
    # Search filters
    col1, col2 = st.columns(2)
    
    with col1:
        search_term = st.text_input("Search by System Name or PWSID:")
        system_type = st.selectbox("System Type:", 
                                  ["", "CWS", "TNCWS", "NTNCWS"])
    
    with col2:
        min_pop = st.number_input("Minimum Population Served:", min_value=0, value=0)
        max_pop = st.number_input("Maximum Population Served:", min_value=0, value=1000000)
    
    # Search button
    if st.button("Search Systems"):
        results = explorer.search_systems(search_term, system_type, min_pop, max_pop)
        
        if not results.empty:
            st.subheader(f"Found {len(results)} systems")
            
            # Make the dataframe interactive
            st.dataframe(
                results,
                column_config={
                    "PWSID": "System ID",
                    "PWS_NAME": "System Name",
                    "PWS_TYPE_CODE": "Type",
                    "POPULATION_SERVED_COUNT": st.column_config.NumberColumn(
                        "Population Served",
                        format="%d"
                    ),
                    "CITY_NAME": "City",
                    "STATE_CODE": "State"
                },
                use_container_width=True
            )
        else:
            st.warning("No systems found matching your criteria.")

def show_violations_analysis(explorer):
    st.header("⚠️ Violations Analysis")
    
    # Violations by type
    violations_by_type = explorer.get_violations_by_type()
    
    if not violations_by_type.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(violations_by_type, x='count', y='VIOLATION_CATEGORY_CODE',
                        orientation='h', title="Violations by Category")
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Health-based violations analysis
            health_violations_query = """
            SELECT IS_HEALTH_BASED_IND, COUNT(*) as count
            FROM violations_enforcement
            WHERE IS_HEALTH_BASED_IND IS NOT NULL
            GROUP BY IS_HEALTH_BASED_IND
            """
            health_violations = explorer.execute_query(health_violations_query)
            
            if not health_violations.empty:
                fig = px.pie(health_violations, values='count', names='IS_HEALTH_BASED_IND',
                           title="Health-Based vs Non-Health-Based Violations")
                st.plotly_chart(fig, use_container_width=True)
    
    # Top violators
    st.subheader("Systems with Most Violations")
    top_violators_query = """
    SELECT v.PWSID, p.PWS_NAME, COUNT(*) as violation_count,
           p.POPULATION_SERVED_COUNT, p.PWS_TYPE_CODE
    FROM violations_enforcement v
    JOIN pub_water_systems p ON v.PWSID = p.PWSID
    GROUP BY v.PWSID, p.PWS_NAME, p.POPULATION_SERVED_COUNT, p.PWS_TYPE_CODE
    ORDER BY violation_count DESC
    LIMIT 20
    """
    top_violators = explorer.execute_query(top_violators_query)
    
    if not top_violators.empty:
        st.dataframe(top_violators, use_container_width=True)

def show_geographic_view(explorer):
    st.header("🗺️ Geographic Distribution")
    
    # Systems by county
    systems_by_county = explorer.get_systems_by_county()
    
    if not systems_by_county.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(systems_by_county.head(15), 
                        x='system_count', y='COUNTY_SERVED',
                        orientation='h', title="Top 15 Counties by Number of Systems")
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.scatter(systems_by_county, 
                           x='system_count', y='avg_population',
                           hover_data=['COUNTY_SERVED'],
                           title="Systems Count vs Average Population by County")
            st.plotly_chart(fig, use_container_width=True)
    
    # Simple map (you would enhance this with actual coordinates)
    st.subheader("Water Systems Map")
    st.info("Map functionality would be enhanced with actual coordinate data for each system.")

def show_system_details(explorer):
    st.header("🏢 System Details")
    
    # PWSID input
    pwsid = st.text_input("Enter PWSID (e.g., GA0000001):")
    
    if pwsid:
        details = explorer.get_system_details(pwsid)
        
        if not details['basic_info'].empty:
            system_info = details['basic_info'].iloc[0]
            
            # Basic information
            st.subheader("Basic Information")
            col1, col2 = st.columns(2)
            
            with col1:
                st.write(f"**System Name:** {system_info['PWS_NAME']}")
                st.write(f"**Type:** {system_info['PWS_TYPE_CODE']}")
                st.write(f"**Population Served:** {system_info['POPULATION_SERVED_COUNT']:,.0f}")
                st.write(f"**Activity Status:** {system_info['PWS_ACTIVITY_CODE']}")
            
            with col2:
                st.write(f"**City:** {system_info['CITY_NAME']}")
                st.write(f"**Owner Type:** {system_info['OWNER_TYPE_CODE']}")
                st.write(f"**Primary Source:** {system_info['PRIMARY_SOURCE_CODE']}")
                st.write(f"**Service Connections:** {system_info['SERVICE_CONNECTIONS_COUNT']}")
            
            # Violations
            if not details['violations'].empty:
                st.subheader("Violations")
                st.dataframe(details['violations'][['VIOLATION_CODE', 'VIOLATION_CATEGORY_CODE', 
                                                  'NON_COMPL_PER_BEGIN_DATE', 'VIOLATION_STATUS']], 
                           use_container_width=True)
            
            # Facilities
            if not details['facilities'].empty:
                st.subheader("Facilities")
                st.dataframe(details['facilities'][['FACILITY_NAME', 'FACILITY_TYPE_CODE', 
                                                  'FACILITY_ACTIVITY_CODE', 'WATER_TYPE_CODE']], 
                           use_container_width=True)
            
            # Site visits
            if not details['site_visits'].empty:
                st.subheader("Recent Site Visits")
                st.dataframe(details['site_visits'][['VISIT_DATE', 'VISIT_REASON_CODE', 
                                                   'COMPLIANCE_EVAL_CODE']], 
                           use_container_width=True)
        else:
            st.error("System not found. Please check the PWSID.")

if __name__ == "__main__":
    main()
