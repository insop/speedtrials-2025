# app.py
import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime

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
                summary_txt="This is a summary of the safety report"
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
                "Enter your city, county, zip code, or water system name:",
                placeholder="e.g., Atlanta, Fulton County, 30309, or City of Atlanta",
                key="search_input"
            )
        
        with col2:
            search_button = st.button("Find My Water System", type="primary")
        
        # Handle search
        if search_button and search_input:
            st.session_state.search_results = explorer.find_water_systems(search_input)
        
        # Display search results
        if not st.session_state.search_results.empty:
            UIComponents._show_search_results(st.session_state.search_results)
        elif search_button and search_input:
            st.warning("No water systems found. Try a different search term.")
        else:
            st.info("💡 **Tip:** Search for your city, county, zip code, or water system name to find your local water system and view its safety report.")
    
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
            summary_txt = SafetyReportGenerator._generate_summary(safety_data, system_info)
        
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
    def _generate_summary(safety_data, system_info):
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
        
        # Generate summary text
        if unresolved_health > 0:
            summary = f"⚠️ **ATTENTION REQUIRED**: This water system serving {pop_text} currently has {unresolved_health} active health-based violation(s). Immediate action may be needed to ensure water safety."
        elif health_violations > 0:
            summary = f"📋 This water system serving {pop_text} has had {health_violations} health-based violation(s) in the past 2 years, but they appear to be resolved. Monitor for updates."
        elif total_violations > 0:
            summary = f"✅ This water system serving {pop_text} has good health compliance but has {total_violations} non-health monitoring/reporting violation(s) in the past 2 years."
        else:
            summary = f"✅ **GOOD NEWS**: This water system serving {pop_text} has no violations in the past 2 years and appears to be operating safely."
        
        # Add test results info if available
        if not test_results_df.empty:
            unique_contaminants = len(test_results_df['CONTAMINANT_CODE'].unique())
            summary += f" Recent testing covers {unique_contaminants} different contaminant(s)."
        
        return summary
    
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
