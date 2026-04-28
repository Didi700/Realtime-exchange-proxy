markdown# Realtime Exchange Rate Proxy

Projet application temps réel — Master 1 Big Data — IRIS Paris

## Contexte
Une entreprise avec 15 000 équipes internes qui appellent chacune 
une API payante de taux de change. Notre solution centralise les 
appels via un proxy Kafka pour réduire les coûts.

## Binôme
- **Fadil** — Producer Kafka + Appel API externe
- **Thomas** — Consumer Kafka + Elasticsearch + Dashboard Kibana

## Architecture
API externe (exchangerate-api.com)
↓ toutes les 30 secondes
[Producer Fadil] — ClientAPI + ProducerPrincipal
↓
Kafka topic : taux-de-change
↓
[Consumer Thomas] — ConsumerPrincipal + ClientElasticsearch
↓
Elasticsearch → Kibana Dashboard

## Technologies
- Apache Kafka 3.6 + Zookeeper
- Elasticsearch 8.11
- Kibana 8.11
- Java 17 / Maven
- Docker Desktop

## Lancement rapide

### 1. Démarrer les services
```bash
docker-compose up -d
```

### 2. Créer le topic Kafka
```bash
docker exec kafka kafka-topics --create \
  --topic taux-de-change \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

### 3. Lancer le Producer (Fadil)
```bash
cd producer
mvn clean package -q
java -jar target/producer-1.0-SNAPSHOT.jar
```

### 4. Lancer le Consumer (Thomas)
```bash
cd consumer
mvn clean package -q
java -jar target/consumer-1.0-SNAPSHOT.jar
```

### 5. Vérifier les données dans Elasticsearch
```bash
curl http://localhost:9200/taux-de-change/_count
```

### 6. Ouvrir Kibana
http://localhost:5601

## Structure du projet
Realtime-exchange-proxy/
├── docker-compose.yml
├── README.md
├── producer/
│   ├── pom.xml
│   └── src/main/java/com/proxy/
│       ├── ClientAPI.java
│       └── ProducerPrincipal.java
└── consumer/
├── pom.xml
└── src/main/java/com/proxy/
├── ClientElasticsearch.java
└── ConsumerPrincipal.java

## Résultats
- Plus de 70 documents indexés dans Elasticsearch
- Taux EUR/USD : 0.853
- Dashboard Kibana avec 3 visualisations en temps réel