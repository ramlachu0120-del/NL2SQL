import re
import sqlite3
from fastapi import FastAPI
from pydantic import BaseModel
from groq import Groq

import os

client = Groq(api_key=os.getenv("GROQ_API_KEY"))

# FastAPI app
app = FastAPI()

# Schema description for the LLM
schema = """
Table daily_revenue(
    DATE TEXT,
    REVENUE REAL,
    COGS REAL,
    FORECASTED_REVENUE REAL,
    Product_id INTEGER,
    Region_id INTEGER
)
"""

# Request body model
class Question(BaseModel):
    question: str


def clean_sql_response(sql_response: str) -> str:
    """
    Cleans the LLM response to extract only the SQL query.
    Removes code block markers and trims whitespace.
    """
    # Remove code block markers like ```sql ... ```
    cleaned = re.sub(r"^```sql\s*|^```|```$", "", sql_response.strip(), flags=re.MULTILINE)
    # Remove any leading/trailing whitespace and newlines
    return cleaned.strip()

def nl_to_sql(question: str):
    print("Generating SQL for question:", question)
    prompt = (
        f"Given the following SQLite table schema:\n{schema}\n"
        f"Convert the following question to a valid SQLite SQL query. "
        f"Return only the SQL query, with no explanation or extra text.\n"
        f"Question: {question}\nSQL:"
    )
    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}]
    )

    res = response.choices[0].message.content.strip()
    cleaned_res = clean_sql_response(res)
    print("Generated SQL:", cleaned_res)
    return cleaned_res

def execute_sql(sql: str):
    conn = sqlite3.connect("demo.db")
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()
    return rows

def sql_result_to_nl(question: str, result):
    print("Generating NL answer for result:", result)
    prompt = f"Question: {question}\nResult: {result}\nAnswer in natural language:"
    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}]
    )
    res = response.choices[0].message.content.strip()
    print("Generated NL answer:", res)
    return res

@app.post("/ask")
async def ask(question: Question):
    try:
        print("Received question:", question.question)
        sql_query = nl_to_sql(question.question)
        result = execute_sql(sql_query)
        answer = sql_result_to_nl(question.question, result)
        return {
            "question": question.question,
            "sql": sql_query,
            "result": result,
            "answer": answer
        }
    except Exception as e:
        print("Error occurred:", e)
        return {
            "error": str(e)
        }

import pandas as pd
import sqlite3
import os

def create_database(db_name):
    conn = sqlite3.connect(db_name)
    return conn

def create_table(conn):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS daily_revenue (
        date TEXT PRIMARY KEY,
        revenue REAL
    );
    """
    conn.execute(create_table_query)
    conn.commit()

def insert_data_from_csv(conn, csv_file):
    df = pd.read_csv(csv_file)
    df.to_sql('daily_revenue', conn, if_exists='replace', index=False)

def main():
    db_name = 'demo.db'
    csv_file = 'daily_revenue.csv'

    if not os.path.exists(csv_file):
        print(f"Error: {csv_file} does not exist.")
        return

    conn = create_database(db_name)
    create_table(conn)
    insert_data_from_csv(conn, csv_file)
    conn.close()
    print(f"Data from {csv_file} has been successfully imported into {db_name}.")

if __name__ == "__main__":
    main()

@app.get("/ping")
def ping():
    return {"message": "pong"}

