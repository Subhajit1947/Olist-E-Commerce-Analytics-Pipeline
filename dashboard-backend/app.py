import time
from datetime import datetime
from functools import lru_cache
from flask import Flask, jsonify, request, render_template,abort
from flask_cors import CORS
import pandas as pd
from config import Config
from s3_reader import S3DataReader

# Initialize Flask app
app = Flask(__name__)
app.config.from_object(Config)

# Enable CORS for all domains (for GitHub Pages)
CORS(app, resources={
    r"/api/*": {
        "origins": ["*"],  # Allow all origins for demo
        "methods": ["GET", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"]
    }
})

# Initialize S3 Reader
s3_reader = S3DataReader(
    bucket_name=app.config['S3_BUCKET'],
    aws_access_key_id=Config.AWS_ACCESS_KEY,
    aws_secret_access_key=Config.AWS_SECRET_KEY)

# Cache for API responses (5 minutes)
@lru_cache(maxsize=100)
def get_cached_data(question_num, cache_key):
    """Cached data retrieval with timestamp"""
    return s3_reader.read_business_answer(question_num)

@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    try:
        # Test S3 connection
        s3_reader.test_connection()
        
        return jsonify({
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "s3_connection": "success",
            "bucket": app.config['S3_BUCKET'],
            "uptime": time.time() - app.start_time
        })
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500
  
@app.route("/api/monthly-revenue")
def get_monthly_revenue():

    year = request.args.get("year", type=int)          
    state = request.args.get("state", type=str)       
    compare_years = request.args.get("compare_years", default="false")
    df = s3_reader.read_business_answer(1) 
    if df.empty:
        abort(500, description="Data not loaded")
    
    filtered_df = df.copy()
    
    # Apply filters
    if year:
        filtered_df = filtered_df[filtered_df['year'] == year]
    if state:
        filtered_df = filtered_df[filtered_df['state'] == state]
    
    if compare_years:
        # Return data for all years comparison
        result = filtered_df.groupby(['year', 'month_name'])['total_revenue'].sum().reset_index()
        
        # Pivot for easier charting
        pivot_df = result.pivot_table(
            index='month_name',
            columns='year',
            values='total_revenue',
            aggfunc='sum'
        ).reset_index()
        
        # Ensure month order
        month_order = ['January', 'February', 'March', 'April', 'May', 'June',
                      'July', 'August', 'September', 'October', 'November', 'December']
        pivot_df['month_name'] = pd.Categorical(pivot_df['month_name'], 
                                                categories=month_order, 
                                                ordered=True)
        pivot_df = pivot_df.sort_values('month_name')
        
        for col in pivot_df.columns:
            if col != 'month_name':
                pivot_df[col] = pivot_df[col].apply(
                    lambda x: None if pd.isna(x) else float(x)
                )
        # Convert to list format
        years = [str(col) for col in pivot_df.columns if col != 'month_name']
        data = []
        
        for _, row in pivot_df.iterrows():
            month_data = {"month": row['month_name']}
            for year_col in years:
                value = round(float(row[int(year_col)]),2) if year_col.isdigit() and not pd.isna(row[int(year_col)]) else 0
                month_data[year_col] = value
            data.append(month_data)
        
        return {
            "data": data,
            "years": years,
            "type": "year_comparison"
        }
    else:
        # Single year or overall
        if not year and not state:
            # Overall monthly average across years
            result = filtered_df.groupby('month_name')['total_revenue'].mean().reset_index()
        else:
            result = filtered_df.groupby('month_name')['total_revenue'].sum().reset_index()
        
        # Sort by month order
        month_order = ['January', 'February', 'March', 'April', 'May', 'June',
                      'July', 'August', 'September', 'October', 'November', 'December']
        result['month_name'] = pd.Categorical(result['month_name'], 
                                              categories=month_order, 
                                              ordered=True)
        result = result.sort_values('month_name')
        
        data = []
        for _, row in result.iterrows():
            data.append({
                "month": row['month_name'],
                "revenue": float(row['total_revenue']),
                "formatted_revenue": f"${row['total_revenue']:,.2f}"
            })
        
        return {
            "data": data,
            "year": year,
            "state": state,
            "type": "single_view"
        }


@app.get("/api/available-filters")
def get_available_filters():
    """Get available years and states for filtering from your CSV data"""
    try:
        df = s3_reader.read_business_answer(1) 
        if df.empty:
            return {
                "years": [],
                "states": [],
                "message": "No data available",
                "status": "error"
            }
        
        # Get unique years (handle NaN and convert to int)
        years_series = df['year'].dropna().unique()
        years = sorted([int(year) for year in years_series if not pd.isna(year)])
        
        # Get unique states (handle NaN)
        states_series = df['state'].dropna().unique()
        states = sorted([str(state).strip() for state in states_series if not pd.isna(state) and str(state).strip()])
        
        # Get month names (for completeness)
        month_series = df['month_name'].dropna().unique()
        months = sorted([str(month).strip() for month in month_series if not pd.isna(month) and str(month).strip()])
        
        # Calculate data range for metadata
        if len(df) > 0:
            min_date = f"{df['year'].min()}"
            max_date = f"{df['year'].max()}"
            total_records = len(df)
            total_revenue = df['total_revenue'].sum() if 'total_revenue' in df.columns else 0
        else:
            min_date = max_date = "N/A"
            total_records = 0
            total_revenue = 0
        
        return {
            "status": "success",
            "years": years,
            "states": states,
            "months": months,
            "metadata": {
                "data_range": {
                    "min_year": min_date,
                    "max_year": max_date
                },
                "total_records": total_records,
                "total_revenue": float(total_revenue),
                "unique_years_count": len(years),
                "unique_states_count": len(states)
            },
            "last_updated": datetime.now().isoformat()
        }
        
    except Exception as e:
        return {
            "status": "error",
            "years": [],
            "states": [],
            "months": [],
            "error": str(e),
            "message": "Failed to load filter data"
        }



app.start_time = time.time()

if __name__ == '__main__':
    # Development server
    app.run(
        host=app.config.get('HOST', '0.0.0.0'),
        port=app.config.get('PORT', 5000),
        debug=app.config.get('DEBUG', False)
    )