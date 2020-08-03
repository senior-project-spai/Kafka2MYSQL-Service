FROM python:3.6-slim-stretch

WORKDIR /root/k2m-service

# Install python package
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# Copy code
COPY src/. .

CMD python3 main.py
