package de.gridka.dcache.nearline.hpss;

import javax.json.Json;
import javax.json.JsonObject;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.logging.LoggingFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diskCacheV111.util.CacheException;

public class TReqS2 {
  private static final Logger LOGGER = LoggerFactory.getLogger(Dc2HpssNearlineStorage.class);
  
  // Jersey logging is build to use java.util.logging (jul), but
  // dCache has log4j framework. 
  private static class JulFacade extends java.util.logging.Logger {
    JulFacade() { super("Jersey", null); }
    @Override public void finest(String msg) { LOGGER.trace(msg); }
    @Override public void finer(String msg) { LOGGER.debug(msg); }
    @Override public void fine(String msg) { LOGGER.debug(msg); }
    @Override public void info(String msg) { LOGGER.info(msg); }
    @Override public void warning(String msg) { LOGGER.warn(msg); }
    @Override public void severe(String msg) { LOGGER.error(msg); }
  }
  
  WebTarget server;
  
  TReqS2 (String server) {
    ClientConfig cfg = new ClientConfig();
    cfg.register(new LoggingFeature(new JulFacade()));
    Client client = ClientBuilder.newClient();
    String serverUri = String.format("http://treqs:changeit@%s:8080/treqs2", server);
    this.server = client.target(UriBuilder.fromUri(serverUri).build());
  }
  
  TReqS2 (String server, String port) {
    ClientConfig cfg = new ClientConfig();
    cfg.register(new LoggingFeature(new JulFacade()));
    Client client = ClientBuilder.newClient();
    String serverUri = String.format("http://treqs:changeit@%s:%s/treqs2", server, port);
    this.server = client.target(UriBuilder.fromUri(serverUri).build());
    // Securing the connection: https://jersey.java.net/documentation/latest/client.html#d0e5229
  }
  
  TReqS2 (String server, String port, String user, String password) {
    ClientConfig cfg = new ClientConfig();
    cfg.register(new LoggingFeature(new JulFacade()));
    Client client = ClientBuilder.newClient();
    String serverUri = String.format("http://%s:%s@%s:%s/treqs2", user, password, server, port);
    this.server = client.target(UriBuilder.fromUri(serverUri).build());
    // Securing the connection: https://jersey.java.net/documentation/latest/client.html#d0e5229
  }
  
  public String initRecall (String hsmPath) throws CacheException {
    LOGGER.debug(String.format("Send stage request for %s to TReqS.", hsmPath));
    JsonObject input = Json.createObjectBuilder().add("file", hsmPath).build();
    Response response = server.path("staging").path("request")
        .request(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.json(input));
    if (response.getStatus() != 200) {
      throw new CacheException(String.format("Failed to initialize recall for %s with TReqS.", hsmPath));
    }
    String id = response.readEntity(JsonObject.class).getString("id");
    LOGGER.debug(String.format("TReqS created request for %s with id '%s'.", hsmPath, id));
    
    return id;
  }

  public JsonObject getStatus (String requestId) {
    LOGGER.debug(String.format("Query status for request '%s'", requestId));
    return server.path("staging").path("request").path(requestId)
        .request(MediaType.APPLICATION_JSON)
        .get(JsonObject.class);
  }

  public void cancelRecall (String hsmPath) {
    LOGGER.debug(String.format("Cancel TReqS requests for %s.", hsmPath));
    server.path("staging").path("file").path(hsmPath)
        .request(MediaType.WILDCARD).delete();
  }
}