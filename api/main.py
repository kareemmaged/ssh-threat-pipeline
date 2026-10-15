import time
from fastapi import FastAPI
from fastapi.responses import StreamingResponse

app = FastAPI()
LOG_FILE = "auth.log"
DELAY_SECONDS = 1  # simulates live log generation; 86 lines ≈ 43 sec


@app.get("/logs/stream")
def stream_logs():
    def generate():
        with open(LOG_FILE) as f:
            for line in f:
                line = line.strip()
                if line:
                    yield f"data: {line}\n\n"
                    time.sleep(DELAY_SECONDS)

    return StreamingResponse(generate(), media_type="text/event-stream")


@app.get("/health")
def health():
    return {"status": "ok"}
