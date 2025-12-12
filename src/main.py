from src.data_loader import load_data
from src.data_cleaner import clean_data
from src.feature_engineering import create_indicators
from src.llm_reasoning import run_economic_analysis
from src.db_insert import insert_into_db

if __name__ == "__main__":
    df = load_data("data/current_account.csv")
    df = clean_data(df)
    df = create_indicators(df)

    insert_into_db(df)   # <— NEW STEP

    insight = run_economic_analysis(df)
    print("\n=== ECONOMIC INSIGHT ===\n")
    print(insight)
