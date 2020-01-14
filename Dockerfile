FROM python:3.7-slim-buster

COPY . /root/k2m-service
RUN cd /root/k2m-service && \
    pip3 install -r requirements.txt

CMD cd /root/k2m-service && \
    python3 mysql_connector_services_new.py
