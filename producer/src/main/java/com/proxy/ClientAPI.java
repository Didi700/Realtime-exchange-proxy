package com.proxy;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Instant;

/**
 * ClientAPI — Fadil
 * ------------------
 * Ce fichier est responsable d'appeler l'API externe de taux de change.
 * Il retourne les données sous forme de JSON enrichi d'un timestamp.
 */
public class ClientAPI {

    // L'adresse de l'API externe que l'on appelle
    private static final String URL_API = "https://api.exchangerate-api.com/v4/latest/USD";

    /**
     * Cette méthode appelle l'API et retourne le JSON des taux.
     * On ajoute aussi la date/heure de la collecte dans le JSON.
     */
    public String recupererTaux() throws Exception {

        // On ouvre une connexion HTTP vers l'API
        URL url = new URL(URL_API);
        HttpURLConnection connexion = (HttpURLConnection) url.openConnection();
        connexion.setRequestMethod("GET");
        connexion.setConnectTimeout(5000); // 5 secondes max pour se connecter
        connexion.setReadTimeout(5000);    // 5 secondes max pour lire la réponse

        // On vérifie que l'API a bien répondu (200 = OK)
        int codeReponse = connexion.getResponseCode();
        if (codeReponse != 200) {
            throw new Exception("Erreur API — code HTTP reçu : " + codeReponse);
        }

        // On lit la réponse ligne par ligne
        BufferedReader lecteur = new BufferedReader(
            new InputStreamReader(connexion.getInputStream())
        );
        StringBuilder contenu = new StringBuilder();
        String ligne;
        while ((ligne = lecteur.readLine()) != null) {
            contenu.append(ligne);
        }
        lecteur.close();

        // On ajoute un champ "timestamp_collecte" pour tracer quand on a collecté
        JsonObject json = JsonParser.parseString(contenu.toString()).getAsJsonObject();
        json.addProperty("timestamp_collecte", Instant.now().toString());

        System.out.println("[ClientAPI] Taux récupérés depuis l'API externe ✓");
        return json.toString();
    }
}