package de.gridka.dcache.nearline.hpss;

import java.io.IOException;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

import diskCacheV111.util.CacheException;

public class TReqS2 {
  private static final Logger LOGGER = LoggerFactory.getLogger(Dc2HpssNearlineStorage.class);
  private String treqsStaging;
  private String user;
  private String password;
  
  TReqS2 (String server, String port, String user, String password) {
    this.treqsStaging = String.format("http://%s:%s/treqs2/staging", server, port);
    this.user = user;
    this.password = password;
    LOGGER.debug("Generated TReqS connectivity string {}", treqsStaging);
  }
  
  public String initRecall (String hsmPath) throws CacheException {
    LOGGER.debug(String.format("Send stage request for %s to TReqS.", hsmPath));
    HttpResponse<JsonNode> response;
    try {
      response = Unirest.post(treqsStaging + "/request")
          .basicAuth(user, password)
          .header("accept", "application/json")
          .header("Content-Type", "application/json")
          .body(String.format("{\"file\":\"%s\"}", hsmPath))
          .asJson();
    } catch (UnirestException e) {
      throw new CacheException(String.format("Submit recall for %s to TReqS failed.", hsmPath), e);
    }
    if (response.getStatus() < 200 || response.getStatus() > 299) {
      throw new CacheException(String.format("Failed to initialize recall for %s with TReqS.\n%s", hsmPath, response.getStatusText()));
    }
    String id = response.getBody().getObject().getString("id");
    LOGGER.debug("TReqS created request for {} with id '{}'.", hsmPath, id);
    
    return id;
  }

  public JSONObject getStatus (String requestId) {
    LOGGER.debug("Query status for request '{}'", requestId);
    try {
      return Unirest.get(treqsStaging + "/request/" + requestId)
          .basicAuth(user, password)
          .header("accept", "application/json")
          .asJson().getBody().getObject();
    } catch (UnirestException e) {
      LOGGER.error("Query status of " + requestId + " failed", e);
      return null;
    }
  }

  public void cancelRecall (String requestId) {
    LOGGER.debug("Cancel TReqS requests for {}.", requestId);
    Unirest.delete(treqsStaging + "/request/" + requestId).basicAuth(user, password);
  }
  
  public void disconnect () {
    try {
      Unirest.shutdown();
    } catch (IOException e) {
      LOGGER.error("Failed to shut down Unirest connection", e);
    }
  }
}