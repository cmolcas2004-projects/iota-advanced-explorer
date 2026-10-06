FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir fastapi "uvicorn[standard]" requests
COPY main.py .
COPY static ./static
ENV DB_PATH=/data/traceability.db
ENV HORNET_URL=http://iota-hornet:14265
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
