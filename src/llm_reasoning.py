import os
import pandas as pd
from dotenv import load_dotenv
import time

# Import both clients safely
try:
    from openai import OpenAI
except ImportError:
    OpenAI = None

try:
    from anthropic import Anthropic
except ImportError:
    Anthropic = None

load_dotenv()


# =============================
#  Client Initialization
# =============================

openai_key = os.getenv("OPENAI_API_KEY")
anthropic_key = os.getenv("ANTHROPIC_API_KEY")

openai_client = OpenAI(api_key=openai_key) if (OpenAI and openai_key) else None
anthropic_client = Anthropic(api_key=anthropic_key) if (Anthropic and anthropic_key) else None


# =============================
#  Core Helper Functions
# =============================

def summarize_data(df: pd.DataFrame, max_rows: int = 5) -> str:
    """Convert numeric DataFrame to text summary."""
    text = df.head(max_rows).to_string(index=False)
    return f"Here are the first {max_rows} rows of the Current Account data:\n{text}"


def analyze_with_gpt(prompt: str) -> str:
    """Send analysis request to OpenAI GPT."""
    response = openai_client.chat.completions.create(
        model="gpt-4o-mini",  # universally available model
        messages=[
            {"role": "system", "content": "You are an expert macroeconomic analyst."},
            {"role": "user", "content": prompt}
        ]
    )
    return response.choices[0].message.content


def analyze_with_claude(prompt: str, retries: int = 3) -> str:
    """Send analysis request to Anthropic Claude with retries."""
    for attempt in range(retries):
        try:
            response = anthropic_client.messages.create(
                model="claude-sonnet-4-5-20250929",
                max_tokens=8000,
                stop_sequences=[],
                messages=[{"role": "user", "content": prompt}]
            )
            return response.content[0].text
        except Exception as e:
            print(f"⚠️ Claude attempt {attempt+1} failed: {e}")
            if "Overloaded" in str(e) and attempt < retries - 1:
                print("⏳ Waiting 5 seconds before retry...")
                time.sleep(5)
            else:
                raise


# =============================
#  Main Economic Reasoning
# =============================

def run_economic_analysis(df: pd.DataFrame) -> str:
    """Automatically decide which LLM to use (Claude or GPT)."""

    # Summarize data for LLM input
    data_summary = summarize_data(df)
    prompt = f"""
        You are an AI Economic Analyst.

        ⚠️ IMPORTANT:
        Your answer MUST be fully complete.
        If the output is long, break it into:
        PART 1
        PART 2
        PART 3
        ...
        Continue until EVERYTHING is fully written.

        Do NOT stop early.
        Do NOT shorten.
        Do NOT say “in summary”.
        Keep writing until the full analysis is complete.

        Provide:
        1. Executive Summary
        2. Trend Analysis
        3. Multi-Year Transition Phases
        4. Key Drivers Behind the Trend
        5. Risk Classification (High/Med/Low)
        6. Structural Causes
        7. Policy Recommendations
        8. Investor Implications
        9. Sector Impact
        10. Forward-looking Forecast (12–24 months)
        11. Final Strategic Advisory

        Dataset:
        {data_summary}
        """


    # Auto model detection
    if anthropic_client:
        print("🧠 Using Claude (Anthropic) for analysis...")
        try:
            return analyze_with_claude(prompt)
        except Exception as e:
            print(f"⚠️ Claude failed: {e}")
            if openai_client:
                print("↩️ Switching to GPT (OpenAI)...")
                return analyze_with_gpt(prompt)
            else:
                raise RuntimeError("No working LLM API available.")
    elif openai_client:
        print("🧠 Using GPT (OpenAI) for analysis...")
        return analyze_with_gpt(prompt)
    else:
        raise RuntimeError("❌ No API key found for either OpenAI or Anthropic.")
