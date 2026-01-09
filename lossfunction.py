import sqlite3
import os.path
import pandas as pd
from datetime import datetime
from datetime import date
import json


def query_database(limit=None, app_filter=None, include_web_usage=True, target_date=None):
    """
    Query macOS Knowledge database for app usage data.
    
    Args:
        limit: Maximum number of records to return
        app_filter: Filter by specific app name (case-insensitive)
        include_web_usage: Include web browsing data with URLs from /app/webUsage
        target_date: Filter by specific date (date object or string 'YYYY-MM-DD')
    
    Returns:
        List of tuples containing app usage data
    """
    # Connect to the SQLite database
    knowledge_db = os.path.expanduser("~/Library/Application Support/Knowledge/knowledgeC.db")
    
    # Check if knowledgeC.db exists
    if not os.path.exists(knowledge_db):
        print(f"Could not find knowledgeC.db at {knowledge_db}.")
        return None
    
    # Check if knowledgeC.db is readable
    if not os.access(knowledge_db, os.R_OK):
        print(f"The knowledgeC.db at {knowledge_db} is not readable.")
        print("Please grant Full Disk Access to your terminal application in System Settings > Privacy & Security.")
        return None
    
    # Connect to the SQLite database
    try:
        with sqlite3.connect(knowledge_db) as con:
            cur = con.cursor()
            
            if include_web_usage:
                # Combined query: App usage + Web usage (with URLs)
                base_query = """
                SELECT
                    ZOBJECT.ZVALUESTRING AS "app", 
                    CASE 
                        WHEN ZSTREAMNAME = '/app/webUsage' THEN ZSTRUCTUREDMETADATA.Z_DKDIGITALHEALTHMETADATAKEY__WEBPAGEURL
                        ELSE NULL
                    END as "url",
                    CASE
                        WHEN ZSTREAMNAME = '/app/webUsage' THEN ZSTRUCTUREDMETADATA.Z_DKDIGITALHEALTHMETADATAKEY__WEBDOMAIN
                        ELSE NULL
                    END as "domain",
                    (ZOBJECT.ZENDDATE - ZOBJECT.ZSTARTDATE) AS "usage",
                    (ZOBJECT.ZSTARTDATE + 978307200) as "start_time", 
                    (ZOBJECT.ZENDDATE + 978307200) as "end_time",
                    (ZOBJECT.ZCREATIONDATE + 978307200) as "created_at", 
                    ZOBJECT.ZSECONDSFROMGMT AS "tz",
                    ZSOURCE.ZDEVICEID AS "device_id",
                    ZMODEL AS "device_model",
                    ZSTREAMNAME as "stream"
                FROM
                    ZOBJECT 
                    LEFT JOIN ZSTRUCTUREDMETADATA 
                        ON ZOBJECT.ZSTRUCTUREDMETADATA = ZSTRUCTUREDMETADATA.Z_PK 
                    LEFT JOIN ZSOURCE 
                        ON ZOBJECT.ZSOURCE = ZSOURCE.Z_PK 
                    LEFT JOIN ZSYNCPEER
                        ON ZSOURCE.ZDEVICEID = ZSYNCPEER.ZDEVICEID
                WHERE
                    ZSTREAMNAME IN ('/app/usage', '/app/webUsage')
                """
            else:
                # Original app usage only query
                base_query = """
                SELECT
                    ZOBJECT.ZVALUESTRING AS "app", 
                    NULL as "url",
                    NULL as "domain",
                    (ZOBJECT.ZENDDATE - ZOBJECT.ZSTARTDATE) AS "usage",
                    (ZOBJECT.ZSTARTDATE + 978307200) as "start_time", 
                    (ZOBJECT.ZENDDATE + 978307200) as "end_time",
                    (ZOBJECT.ZCREATIONDATE + 978307200) as "created_at", 
                    ZOBJECT.ZSECONDSFROMGMT AS "tz",
                    ZSOURCE.ZDEVICEID AS "device_id",
                    ZMODEL AS "device_model",
                    ZSTREAMNAME as "stream"
                FROM
                    ZOBJECT 
                    LEFT JOIN ZSOURCE 
                        ON ZOBJECT.ZSOURCE = ZSOURCE.Z_PK 
                    LEFT JOIN ZSYNCPEER
                        ON ZSOURCE.ZDEVICEID = ZSYNCPEER.ZDEVICEID
                WHERE
                    ZSTREAMNAME = '/app/usage'
                """
            
            # Add app filter if specified
            where_conditions = []
            params = []
            
            # Add app filter if specified
            if app_filter:
                where_conditions.append("ZOBJECT.ZVALUESTRING LIKE ?")
                params.append(f"%{app_filter}%")
            
            # Add date filter if specified
            if target_date:
                # Convert date to string if it's a date object
                if isinstance(target_date, date):
                    date_str = target_date.strftime('%Y-%m-%d')
                else:
                    date_str = target_date
                
                # Filter by date using DATE function on the converted timestamp
                where_conditions.append("DATE((ZOBJECT.ZSTARTDATE + 978307200), 'unixepoch', 'localtime') = ?")
                params.append(date_str)
            
            # Append additional WHERE conditions
            query = base_query
            if where_conditions:
                query += " AND " + " AND ".join(where_conditions)
            
            query += " ORDER BY ZSTARTDATE DESC"
            
            # Add limit if specified
            if limit:
                query += " LIMIT ?"
                params.append(limit)
            
            cur.execute(query, params)
            return cur.fetchall()
            
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return None


def format_results(results):
    if not results:
        print("No results found.")
        return
    
    print(f"{'App':<40} {'URL':<50} {'Usage (s)':<12} {'Start Time':<20}")
    print("-" * 125)
    
    for row in results:
        app, url, domain, usage, start_time, *_ = row
        start_dt = datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')
        url_display = (url[:47] + '...') if url and len(url) > 50 else (url or 'N/A')
        print(f"{app:<40} {url_display:<50} {usage:<12.2f} {start_dt:<20}")


def load_goals(filename='goals.json'):
    """
    Load goals from a JSON file.
    
    Args:
        filename: Path to the goals file
    
    Returns:
        Dictionary of goals or empty dict if file doesn't exist
    """
    try:
        with open(filename, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        print(f"Error reading {filename}. Starting with empty goals.")
        return {}


def save_goals(goals, filename='goals.json'):
    """
    Save goals to a JSON file.
    
    Args:
        goals: Dictionary of goals to save
        filename: Path to save the goals file
    """
    try:
        with open(filename, 'w') as f:
            json.dump(goals, f, indent=2)
        print(f"Goals saved to {filename}")
    except Exception as e:
        print(f"Error saving goals: {e}")


def setgoals(goals=None):
    """
    Set usage goals for apps or domains.
    
    Args:
        goals: Existing dictionary of goals (optional)
    
    Returns:
        Dictionary of app/domain goals with time limits
    """
    # Initialize goals dictionary if not provided
    if goals is None:
        goals = {}
    
    # Show existing goals
    if goals:
        print("\n=== Current Goals ===")
        for app, limit in goals.items():
            print(f"  {app}: {limit}")
        print()
    
    # Get number of goals to add
    try:
        goalnum = int(input("How many new goals would you like to add? "))
    except ValueError:
        print("Invalid number. No goals added.")
        return goals
    
    # Add goals
    for i in range(goalnum):
        print(f"\n--- Goal {i+1} of {goalnum} ---")
        app = input('App/Domain name: ').strip()
        limit = input('Time limit (e.g., "2 hours", "30 minutes"): ').strip()
        
        if app and limit:
            goals[app] = limit
            print(f"✓ Goal set: {app} → {limit}")
        else:
            print("× Skipped (empty input)")
    
    return goals


def simplify_app_name(app_name):
    """
    Simplify app names by removing common prefixes and extensions.
    
    Args:
        app_name: Full app name (e.g., "com.apple.Safari")
    
    Returns:
        Simplified app name (e.g., "Safari")
    """
    if not app_name:
        return app_name
    
    # Remove common bundle identifier prefixes
    prefixes = ['com.apple.', 'com.google.', 'com.microsoft.', 'org.mozilla.', 
                'com.adobe.', 'com.spotify.', 'com.slack.', 'com.']
    
    simplified = app_name
    for prefix in prefixes:
        if simplified.startswith(prefix):
            simplified = simplified[len(prefix):]
            break
    
    # Remove file extensions
    if '.' in simplified:
        # Keep only the first part before the dot (unless it's a domain)
        parts = simplified.split('.')
        if len(parts) > 1 and parts[0]:
            simplified = parts[0]
    
    # Capitalize first letter
    simplified = simplified.capitalize()
    
    return simplified


def simplify_domain_name(domain):
    """
    Simplify domain names by removing common prefixes and keeping main domain.
    
    Args:
        domain: Full domain name (e.g., "www.youtube.com")
    
    Returns:
        Simplified domain name (e.g., "youtube")
    """
    if not domain:
        return domain
    
    # Remove www. prefix
    simplified = domain.replace('www.', '')
    
    # Remove common subdomains
    subdomains_to_remove = ['m.', 'mobile.', 'web.', 'app.', 'api.']
    for subdomain in subdomains_to_remove:
        if simplified.startswith(subdomain):
            simplified = simplified[len(subdomain):]
            break
    
    # Extract main domain (remove TLD)
    # Split by dots and get the second-to-last part (the main domain name)
    parts = simplified.split('.')
    if len(parts) >= 2:
        # For domains like "co.uk", keep more context
        if parts[-1] in ['uk', 'au', 'ca', 'jp'] and len(parts) >= 3:
            simplified = parts[-3]
        else:
            simplified = parts[-2]
    elif len(parts) == 1:
        simplified = parts[0]
    
    # Capitalize first letter
    simplified = simplified.capitalize()
    
    return simplified


def check_goals(df, goals):
    """
    Check if usage exceeds any set goals and display warnings.
    
    Args:
        df: DataFrame with usage data
        goals: Dictionary of goals with time limits
    """
    if not goals:
        return
    
    print("\n=== Goal Check ===")
    
    # Create simplified versions for matching
    df['app_simplified'] = df['app'].apply(simplify_app_name)
    df['domain_simplified'] = df['domain'].apply(simplify_domain_name)
    
    # Aggregate usage by simplified app
    app_usage = df.groupby('app_simplified')['usage_minutes'].sum()
    
    # Aggregate usage by simplified domain (for web usage)
    domain_usage = df[df['domain_simplified'].notna()].groupby('domain_simplified')['usage_minutes'].sum()
    
    for target, limit in goals.items():
        # Check if it's an app
        if target in app_usage.index:
            usage = app_usage[target]
            print(f"{target}: {usage:.1f} minutes (Goal: {limit})")
            
        # Check if it's a domain
        elif target in domain_usage.index:
            usage = domain_usage[target]
            print(f"{target}: {usage:.1f} minutes (Goal: {limit})")
        else:
            print(f"{target}: No usage today (Goal: {limit})")
    
    print()


def update_points(df, goals, points, filename='points.json', points_per_minute=0.5):
    """
    Update points based on goal violations. Deducts points for each goal exceeded.
    Allows points to go negative.
    
    Args:
        df: DataFrame with usage data including 'app_simplified', 'domain_simplified', and 'usage_minutes'
        goals: Dictionary of goals with time limits (e.g., {'Youtube': '2 hours', 'Safari': '30 minutes'})
        points: Dictionary containing current points (e.g., {'Points': 100})
        filename: Path to save the updated points file
        points_per_minute: How many points to lose per minute of overage (default: 0.5)
    
    Returns:
        Updated points dictionary with violations tracked
    """
    if not goals or not points:
        print("No goals or points set. Skipping point updates.")
        return points
    
    # Get current point value
    current_points = points.get('Points', 0)
    violations = []
    
    # Parse time limit string to minutes
    def parse_time_limit(limit_str):
        """Convert time limit string to minutes (e.g., '2 hours' -> 120, '30 minutes' -> 30)"""
        limit_str = limit_str.lower().strip()
        
        if 'hour' in limit_str:
            try:
                hours = float(limit_str.split()[0])
                return hours * 60
            except (ValueError, IndexError):
                return None
        elif 'minute' in limit_str:
            try:
                minutes = float(limit_str.split()[0])
                return minutes
            except (ValueError, IndexError):
                return None
        else:
            # Try to parse as just a number (assume minutes)
            try:
                return float(limit_str)
            except ValueError:
                return None
    
    # Create simplified versions for matching if not already present
    if 'app_simplified' not in df.columns:
        df['app_simplified'] = df['app'].apply(simplify_app_name)
    if 'domain_simplified' not in df.columns:
        df['domain_simplified'] = df['domain'].apply(simplify_domain_name)
    
    # Aggregate usage by simplified app
    app_usage = df.groupby('app_simplified')['usage_minutes'].sum()
    
    # Aggregate usage by simplified domain (for web usage)
    domain_usage = df[df['domain_simplified'].notna()].groupby('domain_simplified')['usage_minutes'].sum()
    
    # Check each goal for violations
    print(f"\n=== Checking for Goal Violations (Rate: {points_per_minute} points/minute) ===")
    
    for target, limit_str in goals.items():
        limit_minutes = parse_time_limit(limit_str)
        
        if limit_minutes is None:
            print(f"⚠ Could not parse time limit for {target}: '{limit_str}'")
            continue
        
        # Check if it's an app
        actual_usage = None
        if target in app_usage.index:
            actual_usage = app_usage[target]
        # Check if it's a domain
        elif target in domain_usage.index:
            actual_usage = domain_usage[target]
        
        if actual_usage is not None:
            if actual_usage > limit_minutes:
                overage = actual_usage - limit_minutes
                points_lost = overage * points_per_minute  # Configurable rate
                current_points -= points_lost
                violations.append({
                    'target': target,
                    'limit': limit_minutes,
                    'actual': actual_usage,
                    'overage': overage,
                    'points_lost': points_lost
                })
                print(f"❌ {target}: {actual_usage:.1f} min (Limit: {limit_minutes:.1f} min) - VIOLATED by {overage:.1f} min → Lost {points_lost:.2f} points")
            else:
                print(f"✓ {target}: {actual_usage:.1f} min (Limit: {limit_minutes:.1f} min) - OK")
        else:
            print(f"✓ {target}: No usage today (Limit: {limit_str}) - OK")
    
    # Update points dictionary (allow negative values)
    points['Points'] = current_points
    
    # Display summary
    print(f"\n=== Points Summary ===")
    if violations:
        total_lost = sum(v['points_lost'] for v in violations)
        print(f"Total violations: {len(violations)}")
        print(f"Total points lost: {total_lost:.2f}")
    else:
        print("No violations! Great job! 🎉")
    
    # Show warning if points are negative
    if current_points < 0:
        print(f"Current points: {points['Points']:.2f} ⚠️  (NEGATIVE)")
    else:
        print(f"Current points: {points['Points']:.2f}")
    
    # Save updated points
    save_points(points, filename)
    
    return points

def save_points(points, filename='points.json'):
    try:
        with open(filename, 'w') as f:
            json.dump(points, f, indent=2)
        print(f"Points saved to {filename}")
    except Exception as e:
        print(f"Error saving points: {e}")

def load_points(filename='points.json'):
    """
    Load points from a JSON file.
    
    Args:
        filename: Path to the goals file
    
    Returns:
        Dictionary of points or empty dict if file doesn't exist
    """
    try:
        with open(filename, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        print(f"Error reading {filename}. Starting with empty points.")
        return {}

def set_points(points=None):
    if points is None:
        points = {}
    if points:
        print("\n=== Current points ===")
        for loss, num in points.items():
            print(f"  {loss}: {num}")
        reset=input("Would you like to reset? (yes/no): ").strip().lower()
        if reset in ['yes', 'y']:
            newnum=int(input("New points?: "))
            points[loss]=newnum
        

        return points
    
    print(f"\n--- Points ---")
    loss="Points"
    try:
        num = int(input("What number of points would you like to set? "))
    except ValueError:
        print("Invalid number. No points added.")
    num = input('Point Goal: ').strip()
        
    if loss and num:
        points[loss] = num
        print(f"✓ Points set: {loss} → {num}")
    else:
        print("Skipped (empty input)")
    
    return points

def main():
    print("=== App Usage Tracker ===\n")
    
    # Query today's usage
    results = query_database(target_date=date.today())
    
    if results:
        # Create pandas DataFrame
        df = pd.DataFrame(results, columns=[
            'app', 'url', 'domain', 'usage_seconds', 'start_time', 'end_time', 
            'created_at', 'timezone', 'device_id', 'device_model', 'stream'
        ])
        
        # Convert timestamps to datetime
        df['start_time'] = pd.to_datetime(df['start_time'], unit='s')
        df['end_time'] = pd.to_datetime(df['end_time'], unit='s')
        df['created_at'] = pd.to_datetime(df['created_at'], unit='s')
        
        # Convert usage to minutes for readability
        df['usage_minutes'] = df['usage_seconds'] / 60
        df['usage_hours'] = df['usage_seconds'] / 3600
        
        # Add simplified columns for better readability
        df['app_simplified'] = df['app'].apply(simplify_app_name)
        df['domain_simplified'] = df['domain'].apply(simplify_domain_name)
        
        # Aggregate by domain (using simplified names)
        dfdomian = df[df['domain_simplified'].notna()].groupby('domain_simplified')['usage_minutes'].sum().reset_index()
        dfdomian = dfdomian.sort_values(by='usage_minutes', ascending=False)
        
        # Aggregate by app (using simplified names)
        dfapp = df.groupby('app_simplified')['usage_minutes'].sum().reset_index()
        dfapp = dfapp.sort_values(by='usage_minutes', ascending=False)
        
        # Display top apps and domains
        print("\n=== Top 10 Apps by Usage ===")
        print(dfapp.head(10).to_string(index=False))
        
        print("\n=== Top 10 Domains by Usage ===")
        print(dfdomian.head(10).to_string(index=False))
        print()
        
        # Save to Excel
        df.to_excel("output.xlsx", index=False)
        dfdomian.to_excel("domain_summary.xlsx", index=False)
        dfapp.to_excel("app_summary.xlsx", index=False)
        print("✓ Data exported to output.xlsx, app_summary.xlsx, and domain_summary.xlsx\n")
        
        # Load existing goals
        goals = load_goals()
        
        # Check current usage against goals
        if goals:
            check_goals(df, goals)
        
        # Ask if user wants to manage goals
        goal_input = input('Would you like to manage goals? (yes/no): ').strip().lower()
        
        if goal_input in ['yes', 'y']:
            goals = setgoals(goals)
            save_goals(goals)
        
        points = load_points()
        points_input = input('Would you like to set points? (yes/no): ').strip().lower()
        if points_input in ['yes', 'y']:
            points = set_points(points)
            save_points(points)
        if goals and points:
            points = update_points(df, goals, points)
    else:
        print("No data retrieved. Please check database access permissions.")
# Usage example
if __name__ == "__main__":
    main()