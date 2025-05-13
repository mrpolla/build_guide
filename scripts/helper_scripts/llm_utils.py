import requests
import time
import os
from dotenv import load_dotenv

# Load env vars
load_dotenv()

# You can configure this via .env or directly here
OLLAMA_API_URL = os.getenv("OLLAMA_API_URL")
print(f"[DEBUG] Using OLLAMA_API_URL: {OLLAMA_API_URL}")
# Optional: limit to a few verified models
MODEL_MAPPING = {
    "mistral": "mistral",
    "llama3": "llama3",
    "gemma:2b": "gemma:2b",
    "qwen:1.8b": "qwen:1.8b",
    "phi3:mini": "phi3:mini",
}

def query_llm(prompt: str, model_name: str = "mistral") -> str:
    """
    Send a prompt to Ollama and return the response.
    
    :param prompt: The user question or input.
    :param model_name: Optional; one of the mapped model keys.
    :return: LLM response string.
    """
    model = MODEL_MAPPING.get(model_name.lower(), "mistral")

    try:
        start_time = time.time()
        response = requests.post(
            OLLAMA_API_URL,
            json={
                "model": model,
                "prompt": prompt,
                "stream": False
            },
            timeout=60
        )
        duration = time.time() - start_time
        print(f"[INFO] Response Time: {duration:.2f}s")

        if response.status_code != 200:
            print(f"[ERROR] Status Code: {response.status_code} | {response.text}")
            raise RuntimeError("Ollama returned non-200 response.")

        result = response.json()
        actual_model = result.get("model", "unknown")
        print(f"[INFO] Requested Model: {model} | Actual Model Used: {actual_model} | Response Time: {duration:.2f}s")
        return result.get("response", "[No response returned]")
    
    except Exception as e:
        print(f"[ERROR] Ollama inference failed: {e}")
        return "[LLM inference error]"
