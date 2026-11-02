
# Simulator and receivers share this image.
FROM python:3.12-slim

WORKDIR /app

# Copy Python source files.
COPY *.py ./

# Include predefined aircraft scenarios.
COPY scenarios/ ./scenarios/

ENV PYTHONUNBUFFERED=1

CMD ["python", "sim.py"]
