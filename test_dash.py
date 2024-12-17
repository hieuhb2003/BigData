import streamlit as st 
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os
import time
from typing import Dict, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure page settings
st.set_page_config(
    page_title="Reddit Analysis Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

class DatabaseConnection:
    """Database connection handler"""
    
    @staticmethod
    def get_connection():
        """Create database connection using SQLAlchemy"""
        try:
            # Create SQLAlchemy engine
            DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/reddit-data"
            engine = create_engine(DATABASE_URL)
            logger.info("Database connection established successfully")
            return engine
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            st.error(f"Database connection failed: {str(e)}")
            return None

class DataLoader:
    """Data loading and processing handler"""
    
    @staticmethod
    def load_data(engine) -> Dict[str, pd.DataFrame]:
        """Load all data at once"""
        try:
            data = {}
            
            # Entity Distribution
            data['entities'] = pd.read_sql("""
                SELECT 
                    entity_type, 
                    COUNT(*) as total_mentions,
                    COUNT(DISTINCT post_id) as unique_posts
                FROM post_entities
                GROUP BY entity_type
            """, engine)
            
            # Top Entities
            data['top_entities'] = pd.read_sql("""
                SELECT 
                    entity_name,
                    entity_type,
                    COUNT(*) as total_mentions,
                    COUNT(DISTINCT post_id) as posts_count
                FROM post_entities
                GROUP BY entity_name, entity_type
                HAVING COUNT(*) > 1
                ORDER BY total_mentions DESC
                LIMIT 15
            """, engine)
            
            # Timeline
            data['timeline'] = pd.read_sql("""
                SELECT 
                    DATE(created_at) as date,
                    entity_type,
                    COUNT(DISTINCT post_id) as unique_posts,
                    COUNT(*) as total_mentions
                FROM post_entities
                GROUP BY DATE(created_at), entity_type
                ORDER BY date
            """, engine)
            
            # Co-occurrence
            data['cooccurrence'] = pd.read_sql("""
                WITH post_entity_counts AS (
                    SELECT post_id, COUNT(DISTINCT entity_name) as entity_count
                    FROM post_entities
                    GROUP BY post_id
                )
                SELECT 
                    entity_count,
                    COUNT(*) as post_count
                FROM post_entity_counts
                GROUP BY entity_count
                ORDER BY entity_count
            """, engine)
            
            return data
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            st.error(f"Error loading data: {str(e)}")
            return None

class Dashboard:
    """Dashboard visualization handler"""
    
    @staticmethod
    def create_entity_distribution_charts(df: pd.DataFrame):
        col1, col2 = st.columns(2)
        with col1:
            # Pie chart
            fig_pie = px.pie(
                df,
                values='total_mentions',
                names='entity_type',
                title='Distribution of Entity Types by Mentions'
            )
            st.plotly_chart(fig_pie, use_container_width=True)
        
        with col2:
            # Bar chart
            fig_bar = go.Figure()
            fig_bar.add_trace(go.Bar(
                name='Total Mentions',
                x=df['entity_type'],
                y=df['total_mentions']
            ))
            fig_bar.add_trace(go.Bar(
                name='Unique Posts',
                x=df['entity_type'],
                y=df['unique_posts']
            ))
            fig_bar.update_layout(
                title='Entity Types: Total Mentions vs Unique Posts',
                barmode='group'
            )
            st.plotly_chart(fig_bar, use_container_width=True)

    @staticmethod
    def create_visualization(data: Dict[str, pd.DataFrame]):
        st.header('Entity Type Distribution')
        Dashboard.create_entity_distribution_charts(data['entities'])
            
        st.header('Top Mentioned Entities')
        fig_top = px.bar(
            data['top_entities'],
            x='entity_name',
            y='total_mentions',
            color='entity_type',
            title='Top 15 Most Mentioned Entities',
            hover_data=['posts_count']
        )
        fig_top.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig_top, use_container_width=True)
        
        st.header('Mentions Timeline')
        fig_timeline = px.line(
            data['timeline'], 
            x='date', 
            y='total_mentions', 
            color='entity_type',
            title='Mentions Over Time by Entity Type'
        )
        st.plotly_chart(fig_timeline, use_container_width=True)
        
        st.header('Entity Co-occurrence')
        fig_cooccurrence = px.bar(
            data['cooccurrence'],
            x='entity_count',
            y='post_count',
            title='Number of Entities per Post Distribution'
        )
        st.plotly_chart(fig_cooccurrence, use_container_width=True)

def main():
    """Main application function"""
    st.title('Reddit Analysis Dashboard')
    
    # Sidebar controls
    st.sidebar.header('Dashboard Controls')
    if st.sidebar.button('🔄 Refresh Data'):
        st.experimental_rerun()
    
    # Last updated time placeholder
    last_updated = st.empty()
    
    try:
        # Get database connection
        engine = DatabaseConnection.get_connection()
        if engine is None:
            return

        # Load all data
        data_loader = DataLoader()
        data = data_loader.load_data(engine)
        
        if data is not None:
            # Update timestamp
            last_updated.write(f"Last updated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Create visualizations
            Dashboard.create_visualization(data)
        
    except Exception as e:
        logger.error(f"Error in dashboard: {e}")
        st.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()