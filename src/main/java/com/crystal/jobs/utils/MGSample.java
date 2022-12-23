package com.crystal.jobs.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;


public class MGSample {
    private static final String MY_DOMAIN_NAME =  "brad@sandboxcc540cf61b54437abaa35b63836725e8.mailgun.org";
    private static final String API_KEY = "eb38c18d-4cdcbdb6";

    // ...
    public static JsonNode sendSimpleMessage()  {
//        ObjectMapper objectMapper=new ObjectMapper();
//        objectMapper.writeValueAsString()
//        HttpResponse<JsonNode> request =

        try {
            System.out.println(Unirest.post("https://api.mailgun.net/v3/" + MY_DOMAIN_NAME + "/messages")
                    .basicAuth("api_key", API_KEY)
                    .queryString("from", "info@conference-app.com")
                    .queryString("to", "stefanruci1997@gmail.com")
                    .queryString("subject", "hello")
                    .queryString("text", "testing")
                    .asJson());
        } catch (UnirestException e) {
            System.out.println("error");
        }
        try {
            return Unirest.post("https://api.mailgun.net/v3/" + MY_DOMAIN_NAME + "/messages")
                            .basicAuth("api_key", API_KEY)
                            .queryString("from", "info@conference-app.com")
                            .queryString("to", "stefanruci1997@gmail.com")
                            .queryString("subject", "hello")
                            .queryString("text", "testing")
                            .asJson()
                            .getBody();
        } catch (UnirestException e) {

            System.out.println("error");
        }
        return null;
//        return request.getBody();
    }

    public static void main(String[] args) {
        sendSimpleMessage();
    }
}