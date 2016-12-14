package de.gridka.dcache.nearline.hpss;

import javax.json.Json;
import javax.json.JsonObject;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;


public class TReqS2 {
  private static final Logger LOGGER = LoggerFactory.getLogger(Dc2HpssNearlineStorage.class);
  WebResource server;
  
  TReqS2 (String server) {
    ClientConfig cfg = new DefaultClientConfig();
    Client client = Client.create(cfg);
    String serverUri = String.format("http://treqs:changeit@%s:8080/treqs2", server);
    this.server = client.resource(UriBuilder.fromUri(serverUri).build());
  }
  
  TReqS2 (String server, String port) {
    ClientConfig cfg = new DefaultClientConfig();
    Client client = Client.create(cfg);
    String serverUri = String.format("http://treqs:changeit@%s:%s/treqs2", server, port);
    this.server = client.resource(UriBuilder.fromUri(serverUri).build());
    // Securing the connection: https://jersey.java.net/documentation/latest/client.html#d0e5229
  }
  
  TReqS2 (String server, String port, String user, String password) {
    ClientConfig cfg = new DefaultClientConfig();
    Client client = Client.create(cfg);
    String serverUri = String.format("http://%s:%s@%s:%s/treqs2", user, password, server, port);
    this.server = client.resource(UriBuilder.fromUri(serverUri).build());
    // Securing the connection: https://jersey.java.net/documentation/latest/client.html#d0e5229
  }
  
  public String initRecall (String hsmPath) {
    LOGGER.debug(String.format("Send stage request for %s to TReqS.", hsmPath));
    JsonObject input = Json.createObjectBuilder().add("file", hsmPath).build();
    JsonObject reply = server.path("staging").path("request")
        .type(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .post(JsonObject.class, input);
    LOGGER.debug(String.format("TReqS created request for %s with id '%s'.", hsmPath, reply.getString("id")));
    
    return reply.getString("id");
  }

  public JsonObject getStatus (String requestId) {
    LOGGER.debug(String.format("Query status for request '%s'", requestId));
    return server.path("staging").path("request")
        .path(requestId)
        .post(JsonObject.class);
  }

  public void cancelRecall (String hsmPath) {
    LOGGER.debug(String.format("Cancel TReqS requests for %s.", hsmPath));
    server.path("staging").path("file").path(hsmPath).delete();
  }
}