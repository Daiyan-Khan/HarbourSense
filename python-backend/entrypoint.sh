#!/bin/sh

# Start manager.py in the background
python manager.py &

# Start FastAPI in the foreground
exec uvicorn main:app --host 0.0.0.0 --port 8000
