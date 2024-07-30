FROM python:3.10.12
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt
CMD ["python3", "app.py"]
EXPOSE 5000