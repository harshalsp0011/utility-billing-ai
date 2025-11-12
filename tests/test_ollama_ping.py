import subprocess, json

OLLAMA_PATH = "/usr/local/bin/ollama"  # full path you just confirmed
prompt = 'Return only JSON: {"ok": true}'

result = subprocess.run(
    [OLLAMA_PATH, "run", "llama3:8b"],
    input=prompt.encode(),
    capture_output=True,
    timeout=60
)

out = result.stdout.decode().strip()
print("Raw output:\n", out[:300])
