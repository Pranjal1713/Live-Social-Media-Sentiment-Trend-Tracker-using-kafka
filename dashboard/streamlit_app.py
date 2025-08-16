import streamlit as st
import pandas as pd
import json
import glob
import os
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import time

# Configure page
st.set_page_config(
    page_title="Social Media Sentiment Dashboard",
    page_icon="📱",
    layout="wide"
)

# Custom CSS
st.markdown("""
<style>
.main-header {
    background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
    padding: 2rem;
    border-radius: 10px;
    margin-bottom: 2rem;
    text-align: center;
}
.main-header h1 {
    color: white;
    margin: 0;
    font-size: 2.5rem;
}
.status-live { color: #28a745; }
.status-offline { color: #dc3545; }
</style>
""", unsafe_allow_html=True)

# Title
st.markdown("""
<div class="main-header">
    <h1>📱 Live Social Media Sentiment Dashboard</h1>
</div>
""", unsafe_allow_html=True)

# Sidebar
st.sidebar.header("⚙️ Settings")

# Data path - simplified
data_paths = [
    "./data/output",
    "data/output", 
    "/opt/spark-data/output"
]

data_path = st.sidebar.selectbox("📂 Data Directory", data_paths)
auto_refresh = st.sidebar.checkbox("🔄 Auto Refresh", value=True)
refresh_interval = st.sidebar.slider("Refresh (seconds)", 3, 30, 5)

# Force refresh button
if st.sidebar.button("🔄 Force Refresh Now"):
    st.cache_data.clear()
    st.rerun()

# Initialize session state
if 'all_data' not in st.session_state:
    st.session_state.all_data = []
if 'processed_files' not in st.session_state:
    st.session_state.processed_files = set()

@st.cache_data(ttl=5)
def load_all_data(data_path, force_reload=False):
    """Load all data from JSON files"""
    try:
        # Debug info
        st.sidebar.write(f"**Checking:** {data_path}")
        st.sidebar.write(f"**Path exists:** {os.path.exists(data_path)}")
        
        if not os.path.exists(data_path):
            st.sidebar.error(f"❌ Path doesn't exist: {data_path}")
            return pd.DataFrame()
        
        # Get all JSON files
        file_patterns = [
            os.path.join(data_path, "part-*.json"),
            os.path.join(data_path, "*.json")
        ]
        
        all_files = []
        for pattern in file_patterns:
            files = glob.glob(pattern)
            all_files.extend(files)
        
        st.sidebar.write(f"**Files found:** {len(all_files)}")
        
        if not all_files:
            st.sidebar.warning("⚠️ No JSON files found")
            # Show what files actually exist
            if os.path.exists(data_path):
                actual_files = os.listdir(data_path)
                st.sidebar.write(f"**Files in directory:** {actual_files}")
            return pd.DataFrame()
        
        # Show found files
        for f in all_files[:5]:
            file_size = os.path.getsize(f) if os.path.exists(f) else 0
            st.sidebar.write(f"  📄 {os.path.basename(f)} ({file_size} bytes)")
        if len(all_files) > 5:
            st.sidebar.write(f"  ... and {len(all_files) - 5} more")
        
        # Read all files
        all_records = []
        
        for file_path in all_files:
            try:
                st.sidebar.write(f"**Reading:** {os.path.basename(file_path)}")
                
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
                
                if not content:
                    st.sidebar.warning(f"Empty file: {os.path.basename(file_path)}")
                    continue
                
                # Parse JSON - try different formats
                records = []
                
                # Method 1: Try JSON Lines (each line is a JSON object)
                lines = content.split('\n')
                for line_num, line in enumerate(lines):
                    line = line.strip()
                    if line:
                        try:
                            record = json.loads(line)
                            records.append(record)
                        except json.JSONDecodeError as e:
                            st.sidebar.error(f"Line {line_num}: {str(e)}")
                
                # Method 2: Try single JSON object
                if not records:
                    try:
                        data = json.loads(content)
                        if isinstance(data, list):
                            records = data
                        elif isinstance(data, dict):
                            records = [data]
                    except json.JSONDecodeError as e:
                        st.sidebar.error(f"JSON parse error: {str(e)}")
                
                if records:
                    all_records.extend(records)
                    st.sidebar.success(f"✅ {len(records)} records from {os.path.basename(file_path)}")
                else:
                    st.sidebar.error(f"❌ No valid JSON in {os.path.basename(file_path)}")
                    # Show first few characters of file for debugging
                    st.sidebar.code(content[:200] + "..." if len(content) > 200 else content)
            
            except Exception as e:
                st.sidebar.error(f"Error reading {os.path.basename(file_path)}: {str(e)}")
        
        st.sidebar.write(f"**Total records loaded:** {len(all_records)}")
        
        if not all_records:
            st.sidebar.error("❌ No valid records found in any file")
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(all_records)
        
        # Show original columns for debugging
        st.sidebar.write(f"**Original columns:** {list(df.columns)}")
        
        # Process the dataframe
        df = process_dataframe(df)
        
        st.sidebar.write(f"**Final DataFrame shape:** {df.shape}")
        
        return df
        
    except Exception as e:
        st.sidebar.error(f"Critical error: {str(e)}")
        return pd.DataFrame()

def process_dataframe(df):
    """Clean and process the dataframe"""
    if df.empty:
        return df
    
    try:
        # Handle timestamps
        for col in ['processing_timestamp', 'timestamp', 'created_at']:
            if col in df.columns:
                try:
                    df[col] = pd.to_datetime(df[col])
                    break
                except:
                    continue
        
        # If no timestamp, create one
        if 'processing_timestamp' not in df.columns:
            df['processing_timestamp'] = datetime.now()
        
        # Fill missing values with defaults
        defaults = {
            'text': 'No text',
            'user': 'anonymous',
            'platform': 'unknown',
            'sentiment_score': 0.0,
            'sentiment_label': 'neutral',
            'likes': 0,
            'retweets': 0,
            'user_followers': 0,
            'city': 'Unknown',
            'country': 'Unknown'
        }
        
        for col, default in defaults.items():
            if col not in df.columns:
                df[col] = default
            else:
                df[col] = df[col].fillna(default)
        
        # Handle hashtags
        if 'hashtags' not in df.columns:
            df['hashtags'] = [[] for _ in range(len(df))]
        else:
            def clean_hashtags(x):
                if pd.isna(x) or x == '':
                    return []
                if isinstance(x, str):
                    if x.startswith('[') and x.endswith(']'):
                        try:
                            return eval(x)
                        except:
                            return []
                    return [x]
                if isinstance(x, list):
                    return x
                return []
            
            df['hashtags'] = df['hashtags'].apply(clean_hashtags)
        
        # Convert numeric columns
        numeric_cols = ['sentiment_score', 'likes', 'retweets', 'user_followers']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        return df
    
    except Exception as e:
        st.sidebar.error(f"Processing error: {str(e)}")
        return df

# Load data
df = load_all_data(data_path)

# Status indicator
col_status, col_info = st.columns([1, 3])
with col_status:
    if not df.empty:
        st.markdown('<p class="status-live">🟢 LIVE DATA</p>', unsafe_allow_html=True)
    else:
        st.markdown('<p class="status-offline">🔴 NO DATA</p>', unsafe_allow_html=True)

with col_info:
    st.info(f"Last updated: {datetime.now().strftime('%H:%M:%S')} | Records: {len(df)}")

# Main dashboard
if not df.empty:
    # Metrics
    st.header("📊 Live Metrics")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📊 Total Posts", f"{len(df):,}")
    with col2:
        st.metric("👍 Avg Likes", f"{df['likes'].mean():,.0f}")
    with col3:
        st.metric("🔄 Avg Retweets", f"{df['retweets'].mean():,.0f}")
    with col4:
        avg_sentiment = df['sentiment_score'].mean()
        emoji = "😊" if avg_sentiment > 0.1 else "😐" if avg_sentiment > -0.1 else "😞"
        st.metric(f"{emoji} Sentiment", f"{avg_sentiment:.3f}")
    
    # Visualizations
    st.header("📈 Visualizations")
    
    # Row 1: Sentiment timeline and distribution
    col1, col2 = st.columns(2)
    
    with col1:
        # Sentiment over time
        if len(df) > 1:
            df_time = df.sort_values('processing_timestamp')
            fig_timeline = px.line(
                df_time, 
                x='processing_timestamp', 
                y='sentiment_score',
                title='Sentiment Score Over Time'
            )
            fig_timeline.update_layout(yaxis_range=[-1, 1])
            st.plotly_chart(fig_timeline, use_container_width=True)
        else:
            st.info("Need more data for timeline")
    
    with col2:
        # Sentiment distribution
        if 'sentiment_label' in df.columns:
            sentiment_counts = df['sentiment_label'].value_counts()
            colors = {'positive': '#28a745', 'negative': '#dc3545', 'neutral': '#6c757d'}
            fig_pie = px.pie(
                values=sentiment_counts.values,
                names=sentiment_counts.index,
                title='Sentiment Distribution',
                color=sentiment_counts.index,
                color_discrete_map=colors
            )
            st.plotly_chart(fig_pie, use_container_width=True)
    
    # Row 2: Platform and hashtags
    col3, col4 = st.columns(2)
    
    with col3:
        # Platform distribution
        if 'platform' in df.columns:
            platform_counts = df['platform'].value_counts()
            fig_platform = px.bar(
                x=platform_counts.index,
                y=platform_counts.values,
                title='Posts by Platform',
                labels={'x': 'Platform', 'y': 'Count'}
            )
            st.plotly_chart(fig_platform, use_container_width=True)
    
    with col4:
        # Top hashtags
        all_hashtags = []
        for hashtags in df['hashtags']:
            if isinstance(hashtags, list):
                all_hashtags.extend(hashtags)
        
        if all_hashtags:
            hashtag_counts = pd.Series(all_hashtags).value_counts().head(10)
            fig_hashtags = px.bar(
                x=hashtag_counts.index,
                y=hashtag_counts.values,
                title='Top 10 Hashtags',
                labels={'x': 'Hashtag', 'y': 'Count'}
            )
            fig_hashtags.update_xaxes(tickangle=45)
            st.plotly_chart(fig_hashtags, use_container_width=True)
        else:
            st.info("No hashtags found")
    
    # Row 3: Country and engagement
    col5, col6 = st.columns(2)
    
    with col5:
        # Country distribution
        if 'country' in df.columns:
            country_counts = df['country'].value_counts().head(10)
            fig_country = px.bar(
                x=country_counts.index,
                y=country_counts.values,
                title='Top 10 Countries',
                labels={'x': 'Country', 'y': 'Posts'}
            )
            st.plotly_chart(fig_country, use_container_width=True)
    
    with col6:
        # Engagement scatter
        sample_df = df.tail(100)  # Last 100 for performance
        fig_scatter = px.scatter(
            sample_df,
            x='likes',
            y='retweets',
            color='sentiment_score',
            size='user_followers',
            title='Engagement Analysis',
            color_continuous_scale='RdYlBu'
        )
        st.plotly_chart(fig_scatter, use_container_width=True)
    
    # Latest posts
    st.header("📝 Latest Posts")
    display_cols = ['processing_timestamp', 'platform', 'user', 'text', 'sentiment_label', 'likes', 'retweets']
    available_cols = [col for col in display_cols if col in df.columns]
    
    latest_df = df[available_cols].tail(10).copy()
    if 'processing_timestamp' in latest_df.columns:
        latest_df['processing_timestamp'] = latest_df['processing_timestamp'].dt.strftime('%H:%M:%S')
    if 'text' in latest_df.columns:
        latest_df['text'] = latest_df['text'].astype(str).str[:80] + '...'
    
    st.dataframe(latest_df.iloc[::-1], use_container_width=True, hide_index=True)
    
    # Raw data sample
    with st.expander("🔍 Raw Data Sample"):
        st.dataframe(df.head(3))
        st.write(f"**Columns:** {list(df.columns)}")
        st.write(f"**Data types:** {dict(df.dtypes)}")

else:
    # No data found
    st.error("❌ No data found!")
    
    st.markdown("### 🔍 Troubleshooting:")
    st.write(f"**Checking path:** `{data_path}`")
    st.write(f"**Path exists:** {os.path.exists(data_path)}")
    
    if os.path.exists(data_path):
        files = os.listdir(data_path)
        st.write(f"**Files in directory:** {files}")
        
        json_files = [f for f in files if f.endswith('.json')]
        st.write(f"**JSON files:** {json_files}")
        
        if json_files:
            sample_file = os.path.join(data_path, json_files[0])
            st.write(f"**Sample file content ({json_files[0]}):**")
            try:
                with open(sample_file, 'r') as f:
                    content = f.read()[:500]  # First 500 chars
                st.code(content)
            except Exception as e:
                st.error(f"Error reading sample file: {e}")
    
    st.markdown("### Expected JSON format:")
    st.code('''{
    "text": "Sample post text",
    "user": "username",
    "platform": "twitter", 
    "sentiment_score": 0.5,
    "sentiment_label": "positive",
    "processing_timestamp": "2024-01-01T12:00:00"
}''')

# Auto refresh
if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()


