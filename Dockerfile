FROM adoptopenjdk/openjdk8:latest

WORKDIR /app

COPY target/ETL.jar /app/ETL.jar

ENV INPUT_PATH="/home/erfan/parquet_data/CDR"
ENV OUTPUT_PATH="/home/erfan/parquet_result/"

CMD ["java", "-jar", "/app/ETL.jar"]