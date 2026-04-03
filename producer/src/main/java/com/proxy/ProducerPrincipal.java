package com.proxy;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * ProducerPrincipal — Fadil
 * --------------------------
 * C'est le point d'entrée du Producer.
 * Ce programme fait 3 choses en boucle toutes les 30 secondes :
 *   1. Appelle l'API externe via ClientAPI
 *   2. Crée un message Kafka avec le JSON reçu
 *   3. Publie ce message sur le topic "taux-de-change"
 */
public class ProducerPrincipal {

    // Nom du topic Kafka sur lequel on envoie les taux
    private static final String NOM_TOPIC  = "taux-de-change";

    // Temps d'attente entre deux appels API (30 secondes)
    private static final int INTERVALLE = 30_000;

    public static void main(String[] args) throws InterruptedException {

        // --- Configuration du Producer Kafka ---
        Properties config = new Properties();

        // Adresse du broker Kafka lancé via Docker
        config.put("bootstrap.servers", "localhost:9092");

        // On envoie des String donc on utilise le StringSerializer
        config.put("key.serializer",
            "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer",
            "org.apache.kafka.common.serialization.StringSerializer");

        // On attend la confirmation d'au moins 1 broker avant de continuer
        config.put("acks", "1");

        // En cas d'échec, on réessaie 3 fois
        config.put("retries", "3");

        // Création du producer et du client API
        KafkaProducer<String, String> producer = new KafkaProducer<>(config);
        ClientAPI clientAPI = new ClientAPI();

        System.out.println("=== Producer démarré ===");
        System.out.println("Topic      : " + NOM_TOPIC);
        System.out.println("Intervalle : " + (INTERVALLE / 1000) + " secondes");
        System.out.println("========================");

        // Boucle infinie : on publie les taux toutes les 30 secondes
        while (true) {
            try {
                // Étape 1 : récupérer les taux depuis l'API externe
                String messageJson = clientAPI.recupererTaux();

                // Étape 2 : créer le message Kafka
                // La clé est "USD" (devise de base), la valeur est le JSON complet
                ProducerRecord<String, String> message =
                    new ProducerRecord<>(NOM_TOPIC, "USD", messageJson);

                // Étape 3 : envoyer le message sur Kafka
                producer.send(message, (RecordMetadata meta, Exception erreur) -> {
                    if (erreur == null) {
                        System.out.println("[Producer] Publié ✓"
                            + " | partition=" + meta.partition()
                            + " | offset="    + meta.offset());
                    } else {
                        System.err.println("[Producer] Échec envoi : "
                            + erreur.getMessage());
                    }
                });

            } catch (Exception e) {
                System.err.println("[Producer] Erreur : " + e.getMessage());
            }

            // On attend 30 secondes avant le prochain appel
            System.out.println("[Producer] Attente de "
                + (INTERVALLE / 1000) + " secondes...\n");
            Thread.sleep(INTERVALLE);
        }
    }
}