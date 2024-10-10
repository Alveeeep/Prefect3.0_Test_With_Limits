FROM python:3.10

RUN apt update && apt install -y curl build-essential libpq-dev sqlite3

RUN pip install --no-cache-dir prefect pandas requests

WORKDIR /app
COPY . .
COPY .env /app/.env
RUN pip install -r requirements.txt

COPY init_prefect.sh /app/init_prefect.sh

RUN chmod a+x /app/*.sh

CMD ["/bin/bash", "-c", "/app/init_prefect.sh"]
