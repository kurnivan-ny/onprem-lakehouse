FROM python:3.11-slim
RUN pip install --no-cache-dir "boto3>=1.34"
COPY infra/images/platform-init/init.py /init.py
CMD ["python", "-u", "/init.py"]
