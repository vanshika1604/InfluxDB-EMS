FROM python:3.8-slim-buster
WORKDIR /app
COPY requirements.txt ./
RUN pip install -r requirements.txt
EXPOSE 3005
COPY . .
CMD [ "python3", "-u", "./app.py" ]