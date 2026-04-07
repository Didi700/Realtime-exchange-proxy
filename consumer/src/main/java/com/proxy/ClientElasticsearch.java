package com.proxy;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.StringReader;

/**
 * ClientElasticsearch — Thomas
 * -----------------------------
 * Ce fichier gère la connexion à Elasticsearch
 * et l'indexation de chaque message reçu depuis Kafka.
 */
public class ClientElasticsearch {

    // Nom de l'index dans Elasticsearch
    // C'est comme le nom d'une table en base de données
    private static final String NOM_INDEX = "taux-de-change";

    // Le client officiel Elasticsearch Java
    private final ElasticsearchClient client;

    /**
     * Constructeur : on se connecte à Elasticsearch au démarrage.
     * Elasticsearch tourne sur localhost:9200 via Docker.
     */
    public ClientElasticsearch() {

        // Client REST bas niveau : gère les appels HTTP vers Elasticsearch
        RestClient restClient = RestClient
            .builder(new HttpHost("localhost", 9200, "http"))
            .build();

        // Transport : fait le lien entre le client Java et le client REST
        ElasticsearchTransport transport = new RestClientTransport(
            restClient, new JacksonJsonpMapper()
        );

        // Client haut niveau : c'est lui qu'on utilise pour indexer
        this.client = new ElasticsearchClient(transport);

        System.out.println("[Elasticsearch] Connexion établie → localhost:9200 ✓");
    }

    /**
     * Indexe un document JSON dans Elasticsearch.
     * On laisse Elasticsearch générer l'ID automatiquement.
     *
     * @param documentJson — le JSON complet reçu depuis Kafka
     */
    public void indexerDocument(String documentJson) throws Exception {

        // Création de la requête d'indexation
        IndexRequest<Object> requete = IndexRequest.of(b -> b
            .index(NOM_INDEX)
            .withJson(new StringReader(documentJson))
        );

        // Envoi du document à Elasticsearch
        var reponse = client.index(requete);

        System.out.println("[Elasticsearch] Document indexé ✓"
            + " | index=" + reponse.index()
            + " | id="    + reponse.id());
    }
}