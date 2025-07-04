FROM python:3.10-slim
WORKDIR /app
COPY app.py .
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh
RUN pip install flask requests redis
EXPOSE 5000
CMD ["./entrypoint.sh"]