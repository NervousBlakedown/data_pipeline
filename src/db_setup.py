# src/db_setup.py
import sqlite3
def setup_database():
    conn = sqlite3.connect('data/events.db')
    cursor = conn.cursor()

    # Create table if it doesn't exist
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS events (
        event_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        event_type TEXT NOT NULL,
        event_value TEXT NOT NULL,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # Insert sample data
    sample_data = [
        (1, 'login', 'User logged in'),
        (1, 'page_view', 'User viewed homepage'),
        (2, 'feature_use', 'User used chat feature'),
        (2, 'error', '404 Page not found')
    ]
    cursor.executemany('INSERT INTO events (user_id, event_type, event_value) VALUES (?, ?, ?)', sample_data)

    conn.commit()
    conn.close()

if __name__ == "__main__":
    setup_database()
