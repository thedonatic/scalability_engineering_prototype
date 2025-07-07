FROM python:3.10-slim
WORKDIR /app
COPY load_balancer.py .
RUN pip install flask requests
EXPOSE 8000
CMD ["python", "load_balancer.py"]
