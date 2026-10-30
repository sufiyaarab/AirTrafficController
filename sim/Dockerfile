# Simulator and ground receivers share this image; compose picks the script.
FROM python:3.12-slim
WORKDIR /app
COPY *.py ./
ENV PYTHONUNBUFFERED=1
CMD ["python", "sim.py"]
