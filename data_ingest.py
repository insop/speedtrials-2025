# data_ingestion.py
import pandas as pd
import sqlite3
import os
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SDWISDataIngestion:
    def __init__(self, csv_directory, db_path="sdwis_georgia.db"):
        self.csv_directory = Path(csv_directory)
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        
        # Define the CSV files and their corresponding table names
        self.file_mappings = {
            'SDWA_EVENTS_MILESTONES.csv': 'events_milestones',
            'SDWA_FACILITIES.csv': 'facilities',
            'SDWA_GEOGRAPHIC_AREAS.csv': 'geographic_areas',
            'SDWA_LCR_Samples.csv': 'lcr_samples',
            'SDWA_PN_VIOLATION_ASSOC.csv': 'pn_violation_assoc',
            'SDWA_PUB_WATER_SYSTEMS.csv': 'pub_water_systems',
            'SDWA_REF_ANSI_AREAS.csv': 'ref_ansi_areas',
            'SDWA_REF_CODE_VALUES.csv': 'ref_code_values',
            'SDWA_SERVICE_AREAS.csv': 'service_areas',
            'SDWA_SITE_VISITS.csv': 'site_visits',
            'SDWA_VIOLATIONS_ENFORCEMENT.csv': 'violations_enforcement'
        }
    
    def clean_dataframe(self, df):
        """Clean and standardize the dataframe"""
        # Convert date columns
        date_columns = [col for col in df.columns if 'DATE' in col.upper()]
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Clean numeric columns
        numeric_columns = [col for col in df.columns if 'COUNT' in col.upper() or 'MEASURE' in col.upper()]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    
    def ingest_data(self):
        """Ingest all CSV files into the database"""
        for csv_file, table_name in self.file_mappings.items():
            file_path = self.csv_directory / csv_file
            
            if file_path.exists():
                logger.info(f"Processing {csv_file}...")
                try:
                    df = pd.read_csv(file_path, low_memory=False)
                    df = self.clean_dataframe(df)
                    
                    # Write to database
                    df.to_sql(table_name, self.conn, if_exists='replace', index=False)
                    logger.info(f"Successfully loaded {len(df)} records into {table_name}")
                    
                except Exception as e:
                    logger.error(f"Error processing {csv_file}: {str(e)}")
            else:
                logger.warning(f"File not found: {csv_file}")
    
    def create_indexes(self):
        """Create indexes for better query performance"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_pws_pwsid ON pub_water_systems(PWSID)",
            "CREATE INDEX IF NOT EXISTS idx_violations_pwsid ON violations_enforcement(PWSID)",
            "CREATE INDEX IF NOT EXISTS idx_facilities_pwsid ON facilities(PWSID)",
            "CREATE INDEX IF NOT EXISTS idx_geographic_pwsid ON geographic_areas(PWSID)",
            "CREATE INDEX IF NOT EXISTS idx_violations_date ON violations_enforcement(NON_COMPL_PER_BEGIN_DATE)",
            "CREATE INDEX IF NOT EXISTS idx_pws_type ON pub_water_systems(PWS_TYPE_CODE)",
            "CREATE INDEX IF NOT EXISTS idx_pws_population ON pub_water_systems(POPULATION_SERVED_COUNT)"
        ]
        
        for index_sql in indexes:
            try:
                self.conn.execute(index_sql)
                logger.info(f"Created index: {index_sql.split('idx_')[1].split(' ')[0]}")
            except Exception as e:
                logger.error(f"Error creating index: {str(e)}")
        
        self.conn.commit()
    
    def add_geographic_data(self):
        """Add additional geographic data for mapping"""
        # This would be where you'd add county coordinates, etc.
        # For now, we'll create a simple county mapping
        
        county_coords = {
            'FULTON': {'lat': 33.7490, 'lon': -84.3880},
            'DEKALB': {'lat': 33.7673, 'lon': -84.2806},
            'GWINNETT': {'lat': 33.9526, 'lon': -84.0807},
            'COBB': {'lat': 33.8839, 'lon': -84.5144},
            # Add more counties as needed
        }
        
        # Create a counties reference table
        counties_df = pd.DataFrame([
            {'county_name': name, 'latitude': coords['lat'], 'longitude': coords['lon']}
            for name, coords in county_coords.items()
        ])
        
        counties_df.to_sql('county_coordinates', self.conn, if_exists='replace', index=False)
        logger.info("Added county coordinate data")
    
    def close(self):
        self.conn.close()

if __name__ == "__main__":
    # Usage
    ingestion = SDWISDataIngestion("data/")
    ingestion.ingest_data()
    ingestion.create_indexes()
    ingestion.add_geographic_data()
    ingestion.close()
