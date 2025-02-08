FROM python:3.9-slim-bullseye

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Install Tesseract and its dependencies
RUN apt-get update && apt-get install -y tesseract-ocr libtesseract-dev

COPY . .

CMD ["functions-framework", "serve"]
