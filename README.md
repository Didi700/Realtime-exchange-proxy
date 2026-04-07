# Realtime-exchange-proxy
Projet application temps réel Kafka - Elasticsearch - Kibana
# Exchange Rate Proxy — Projet Temps Réel

Master 1 Big Data — IRIS Paris

## Binôme
- **Fadil** — Producer Kafka + Appel API externe
- **Thomas** — Consumer Kafka + Elasticsearch + Dashboard Kibana

## Architecture
API externe → [Producer Fadil] → Kafka (topic: taux-de-change)
                                          ↓
                                [Consumer Thomas]
                                          ↓
                                Elasticsearch → Kibana

## Lancement rapide
docker-compose up -d
docker exec kafka kafka-topics --create --topic taux-de-change
  --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

## Technologies
- Apache Kafka 3.6 / Zookeeper
- Elasticsearch 8.11 + Kibana 8.11
- Java 17 / Maven 3.9
- Docker Desktops