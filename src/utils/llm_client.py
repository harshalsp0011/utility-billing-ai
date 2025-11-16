import os
from openai import OpenAI
from src.utils.config import OPENAI_API_KEY, OPENAI_MODEL

class LLMClient:
    def __init__(self, api_key=OPENAI_API_KEY, model=OPENAI_MODEL):
        self.client = OpenAI(api_key=api_key)
        self.model = model

    def ask(self, prompt, temperature=0.0):
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            temperature=temperature
        )
        return response.choices[0].message.content

# Global shared instance for all agents
llm = LLMClient()


## use this code to use the LLMClient directly
#from src.llm_client import llm