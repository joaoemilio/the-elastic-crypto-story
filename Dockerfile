FROM python:3.9.7
ENV DAY="20210901"
COPY requirements.txt /
RUN pip3 install -r /requirements.txt
COPY src/ /app
WORKDIR /app
CMD ["sh", "-c", "python3 EnrichSymbolData.py $DAY" ]