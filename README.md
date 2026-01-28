# smart-city-traffic-pipeline
An end-to-end real-time big data pipeline for smart city traffic monitoring using Kafka, Spark Structured Streaming, Airflow, and Docker.

docker-compose up -d
docker ps
<!-- Create Topics -->
docker exec -it kafka kafka-topics --create --topic traffic-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

docker exec -it kafka kafka-topics --create --topic critical-traffic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1


docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

python -m venv venv
venv\Scripts\active
pip install -r requirements.txt or pip install kafka-python
python producer/traffic_producer.py
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic traffic-data --from-beginning


docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/bitnami/spark/apps/traffic_stream.py
