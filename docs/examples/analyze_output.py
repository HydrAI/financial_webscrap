"""Load and analyze scraper output with pandas."""

import sys
from pathlib import Path

import pandas as pd


def main():
    if len(sys.argv) < 2:
        print("Usage: python analyze_output.py <path/to/output.parquet>")
        sys.exit(1)

    path = Path(sys.argv[1])
    df = pd.read_parquet(path)

    print(f"Records: {len(df)}")
    print(f"Columns: {list(df.columns)}")
    print(f"Unique sources: {df['source'].nunique()}")
    print(f"Unique queries: {df['company'].nunique()}")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    print()

    print("Top 10 sources:")
    print(df["source"].value_counts().head(10).to_string())
    print()

    print("Average text length by source (top 10):")
    df["text_len"] = df["full_text"].str.len()
    avg_len = df.groupby("source")["text_len"].mean().sort_values(ascending=False)
    print(avg_len.head(10).to_string())


if __name__ == "__main__":
    main()
