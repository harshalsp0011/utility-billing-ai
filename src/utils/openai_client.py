import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

# Load from environment
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

# Create reusable client
client = OpenAI(api_key=OPENAI_API_KEY)

def ask_gpt(prompt: str):
    """
    Reusable helper to send a prompt to GPT-4.1-mini.
    Returns model text response.
    """
    response = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2
    )

    return response.choices[0].message.content
