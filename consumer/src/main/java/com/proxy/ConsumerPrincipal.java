package com.proxy;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * ConsumerPrincipal — Thomas
 * ---------------------------
 * C'est le point d'entrée du Consumer.
 * Ce programme fait 2 choses en boucle permanente :
 *   1. Écoute le topic Kafka "taux-de-change"
 *   2. Pour chaque message reçu, l'indexe dans Elasticsearch
 */
public class ConsumerPrincipal {

    // Même topic que le Producer de Fadil
    private static final String NOM_TOPIC = "taux-de-change";

    public static void main(String[] args) {

        // --- Configuration du Consumer Kafka ---
        Properties config = new Properties();

        // Adresse du broker Kafka (lancé via Docker)
        config.put("bootstrap.servers", "localhost:9092");

        // Identifiant du groupe de consommateurs
        // Tous les consumers d'un même groupe se partagent les partitions
        config.put("group.id", "groupe-consumer-proxy");

        // On reçoit des String donc on utilise le StringDeserializer
        config.put("key.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer");

        // "earliest" : si on n'a jamais lu ce topic, on repart du début
        config.put("auto.offset.reset", "earliest");

        // Validation automatique de l'offset toutes les 5 secondes
        config.put("enable.auto.commit", "true");
        config.put("auto.commit.interval.ms", "5000");

        // Création du consumer Kafka
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);

        // Abonnement au topic de Fadil
        consumer.subscribe(Collections.singletonList(NOM_TOPIC));

        // Création du client Elasticsearch
        ClientElasticsearch clientES = new ClientElasticsearch();

        System.out.println("=== Consumer démarré ===");
        System.out.println("Abonné au topic : " + NOM_TOPIC);
        System.out.println("Groupe          : groupe-consumer-proxy");
        System.out.println("========================");

        // Boucle infinie : on écoute les messages en permanence
        while (true) {

            // On attend maximum 1 seconde si aucun message n'arrive
            ConsumerRecords<String, String> messages =
                consumer.poll(Duration.ofMillis(1000));

            // Pour chaque message reçu depuis Kafka
            for (ConsumerRecord<String, String> message : messages) {

                System.out.println("[Consumer] Message reçu ✓"
                    + " | partition=" + message.partition()
                    + " | offset="    + message.offset()
                    + " | clé="       + message.key());

                try {
                    // On indexe le message dans Elasticsearch
                    clientES.indexerDocument(message.value());

                } catch (Exception e) {
                    System.err.println("[Consumer] Erreur indexation : "
                        + e.getMessage());
                }
            }
        }
    }
}