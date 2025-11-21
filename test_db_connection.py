from dotenv import load_dotenv
load_dotenv()

import os
from sqlalchemy import create_engine, text

def main():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("❌ DATABASE_URL is not set")
        return

    print(f"Using DATABASE_URL: {database_url[:60]}...")  # don't print full URL in logs

    try:
        engine = create_engine(database_url)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
            print(f"✅ DB connection OK, SELECT 1 returned: {result}")
    except Exception as e:
        print("❌ Failed to connect to the database:")
        print(repr(e))

if __name__ == "__main__":
    main()
