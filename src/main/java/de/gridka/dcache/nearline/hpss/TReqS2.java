package de.gridka.dcache.nearline.hpss;

import javax.json.JsonObject;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;


public class TReqS2 {
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
    return server.path("staging").path("request")
        .entity("{file:" + hsmPath + "}", MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .post(JsonObject.class)
        .getString("id");
  }

  public JsonObject getStatus (String requestId) {
    return server.path("staging").path("request")
        .path(requestId)
        .post(JsonObject.class);
  }

  public void cancelRecall (String hsmPath) {
    server.path("staging").path("file").path(hsmPath).delete();
  }
}