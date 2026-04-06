from flask import Flask, render_template, jsonify, request
from datetime import datetime
import pandas as pd
import sqlite3
import os
from threading import Lock

app = Flask(__name__)

# Global variables
csv_file = 'log.csv'
data_lock = Lock()
latest_data = []

 

def read_db_data(db_path='logs/production_remote.db'):
    """Read and parse data from SQLite database"""
    try:
        if os.path.exists(db_path):
            conn = sqlite3.connect(db_path)
            query = "SELECT * FROM production_log"
            df = pd.read_sql_query(query, conn)
            # Parse timestamp to datetime
            df['datetime'] = pd.to_datetime(df['timestamp'])
            conn.close()
            return df.to_dict('records')
        return []
    except Exception as e:
        print(f"Error reading DB: {e}")
        return []

def calculate_oil_in(data, start_datetime=None):
    """Calculate accumulated oil in values"""
    total_oil_in = 0
    oil_entries = []
    
    for entry in data:
        entry_datetime = entry.get('datetime')
        
        # Filter by start datetime if provided
        if start_datetime and entry_datetime < start_datetime:
            continue
        
        initial_val = float(entry.get('initial_weight', 0))
        final_val = float(entry.get('final_weight', 0))
        req_val = float(entry.get('required_weight', 0))
        
        # Oil in occurs when final > initial (oil was added)
        if final_val > initial_val:
            oil_in = final_val - initial_val
            total_oil_in += oil_in
            
            oil_entries.append({
                'datetime': entry_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                'product': entry.get('product', ''),
                'initial_weight': initial_val,
                'required_weight': req_val,
                'final_weight': final_val,
                'oil_in': oil_in,
                'status': entry.get('status', '')
            })
    
    # Calculate average per entry
    average_per_entry = total_oil_in / len(oil_entries) if len(oil_entries) > 0 else 0
    
    return {
        'total_oil_in': round(total_oil_in, 2),
        'entries': oil_entries,
        'count': len(oil_entries),
        'average_per_entry': round(average_per_entry, 2)
    }

def calculate_oil_in_from_db(data, start_datetime=None):
    """Calculate accumulated oil in values from database"""
    # data is the db_path
    db_data = read_db_data(data)
    return calculate_oil_in(db_data, start_datetime)

@app.route('/')
def index():
    """Render main page"""
    return render_template('index.html')

@app.route('/api/data')
def get_data():
    """Get current data"""
    global latest_data
    
    with data_lock:
        latest_data = read_db_data()
        
        # Get start_datetime from query params if provided
        start_datetime_str = request.args.get('start_datetime')
        start_datetime = None
        
        if start_datetime_str:
            try:
                start_datetime = datetime.strptime(start_datetime_str, '%Y-%m-%dT%H:%M')
            except:
                pass
        else:
            # Use same day 12am
            now = datetime.now()
            start_datetime = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        result = calculate_oil_in(latest_data, start_datetime)
        
        # Get min and max datetime for the date picker
        if latest_data:
            min_datetime = min(entry['datetime'] for entry in latest_data)
            max_datetime = max(entry['datetime'] for entry in latest_data)
            result['min_datetime'] = min_datetime.strftime('%Y-%m-%dT%H:%M')
            result['max_datetime'] = max_datetime.strftime('%Y-%m-%dT%H:%M')
        
        return jsonify(result)


@app.route('/api/stats') 
def get_stats():
    """Get statistics"""
    with data_lock:
        if not latest_data:
            latest_data = read_csv_data()
        
        if not latest_data:
            return jsonify({'total_records': 0})
        
        total_records = len(latest_data)
        
        return jsonify({
            'total_records': total_records,
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)