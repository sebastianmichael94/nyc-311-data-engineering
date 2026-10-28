import psycopg

def main():
    with psycopg.connect(
        host="localhost",
        port=5433,
        dbname="nyc_311",
        user="pipeline_user",
        password="local_dev_password",
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database(), current_user;")
            result = cur.fetchone()
            print(f"Connected successfully: {result}")

if __name__ == "__main__":
    main()