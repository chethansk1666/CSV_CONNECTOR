# main.py

from fastapi import FastAPI
from routers import canvas_router, block_router  # Import routers
import uvicorn

app = FastAPI()

# Include routers
app.include_router(canvas_router.router)
app.include_router(block_router.router)

if __name__ == "__main__":
     # Start the FastAPI application using uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)