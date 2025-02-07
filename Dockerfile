FROM databricksruntime/standard:latest

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY databricks_pipeline.py .

CMD ["python", "databricks_pipeline.py"]
