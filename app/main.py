from fastapi import FastAPI
from app.routes import chat

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Hello, FastAPI!"}

# Include chat router
app.include_router(chat.router, prefix="/chat")
