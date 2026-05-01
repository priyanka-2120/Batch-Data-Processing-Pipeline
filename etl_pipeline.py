import pandas as pd
import mysql.connector

# Step 1: Extract
def extract_data(file_path):
    df = pd.read_csv(file_path)
    print("Data extracted successfully")
    return df

# Step 2: Transform
def transform_data(df):
    # Remove null values
    df = df.dropna()

    # Example transformation
    df['total_engagement'] = df['likes'] + df['comments']

    # Remove duplicates
    df = df.drop_duplicates()

    print("Data transformed successfully")
    return df

# Step 3: Load
def load_data(df):
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="password",
        database="data_engineering"
    )
    
    cursor = conn.cursor()

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO youtube_data (video_id, views, likes, comments, total_engagement)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            row['video_id'],
            int(row['views']),
            int(row['likes']),
            int(row['comments']),
            int(row['total_engagement'])
        ))

    conn.commit()
    conn.close()
    print("Data loaded into MySQL successfully")

# Main pipeline
if __name__ == "__main__":
    file_path = "data/raw_data.csv"
    
    df = extract_data(file_path)
    df = transform_data(df)
    load_data(df)
