FROM adoptopenjdk/openjdk8:latest

WORKDIR /app

COPY target/ETL.jar /app/ETL.jar

# Set environment variables
# ENV INPUT_PATH="/home/erfan/parquet_data/CDR"
# ENV OUTPUT_PATH="/home/erfan/parquet_result/"

# Use ENTRYPOINT to ensure additional arguments are forwarded
ENTRYPOINT ["java", "-jar", "/app/ETL.jar"]
