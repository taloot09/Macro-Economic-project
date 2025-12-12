# src/db_insert.py
import psycopg2
import pandas as pd

def insert_into_db(df: pd.DataFrame):
    """Insert melted economic data into Postgres DB (handles description column name)."""
    try:
        conn = psycopg2.connect(
            dbname="current_account_db",
            user="postgres",
            password="abcore123",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()

        # ✅ Check if data is already in long format
        if 'date' in df.columns and 'value' in df.columns and 'description' in df.columns:
            print("📋 Data is already in long format. Skipping melt.")
            melted = df.copy()
            desc_col = 'description'
            
            # ✅ Parse the date column (it's still in "Jul-13" format)
            def parse_month(x):
                try:
                    # Try standard format first
                    return pd.to_datetime(x, format="%b-%y")
                except:
                    try:
                        # Try without format
                        return pd.to_datetime(x)
                    except:
                        return None
            
            melted["date"] = melted["date"].apply(parse_month)
            
        else:
            # find description column (case-insensitive)
            desc_col = None
            for c in df.columns:
                if c.lower() == "description":
                    desc_col = c
                    break
            if desc_col is None:
                print("❌ 'description' column missing. Cannot insert.")
                return

            # Prepare for melting
            safe_df = df.copy()
            if "value" in safe_df.columns:
                safe_df.rename(columns={"value": "value_original"}, inplace=True)

            # Get only date columns (exclude Description)
            date_columns = [col for col in safe_df.columns if col != desc_col]

            # Melt only date columns
            melted = safe_df.melt(
                id_vars=[desc_col],
                value_vars=date_columns,
                var_name="month",
                value_name="value"
            )

            # Convert month (e.g. Jul-13) → date
            def parse_month(x):
                try:
                    return pd.to_datetime(x, format="%b-%y")
                except Exception:
                    try:
                        return pd.to_datetime(x)
                    except Exception:
                        return None

            melted["date"] = melted["month"].apply(parse_month)

        # ✅ Filter out null dates and invalid values BEFORE loop
        melted = melted[melted["date"].notna()]
        
        # ✅ Filter out non-numeric values
        melted = melted[pd.to_numeric(melted["value"], errors='coerce').notna()]
        melted["value"] = pd.to_numeric(melted["value"])

        # DEBUG: Check data before inserting
        print(f"🔍 Total rows in melted data: {len(melted)}")
        print(f"🔍 Null dates: {melted['date'].isna().sum()}")
        print(f"🔍 Sample data:")
        print(melted.head(10))

        # Insert rows
        inserted_count = 0

        for _, row in melted.iterrows():
            
            # Print first 5 inserts only
            if inserted_count < 5:
                print(f"✅ Inserting: {row[desc_col][:40]}... | {row['date']} | {row['value']}")

            cursor.execute("""
                INSERT INTO economic_indicators (description, indicator_category, date, value)
                VALUES (%s, %s, %s, %s)
            """, (
                row[desc_col],
                None,
                row["date"],
                float(row["value"])
            ))
            inserted_count += 1

        conn.commit()
        cursor.close()
        conn.close()

        print(f"📊 Total rows inserted: {inserted_count}")
        print("📥 Data inserted into Postgres successfully.")

    except Exception as e:
        print(f"❌ DB Insert Error: {e}")
        import traceback
        traceback.print_exc()