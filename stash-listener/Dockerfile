FROM python:3.12

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY db.py listener.py login.py search.py ./

CMD ["python", "-u", "listener.py"]
