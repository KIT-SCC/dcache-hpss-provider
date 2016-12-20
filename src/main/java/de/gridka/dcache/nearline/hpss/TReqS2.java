package de.gridka.dcache.nearline.hpss;

import java.io.IOException;
import java.util.logging.Level;

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
  
  TReqS2 (String server, String port, String user, String password) {
    this.treqsStaging = String.format("http://%s:%s@%s:%s/treqs2/staging", user, password, server, port);
    LOGGER.trace("Generated TReqS connectivity string %s", treqsStaging);
  }
  
  public String initRecall (String hsmPath) throws CacheException {
    LOGGER.debug(String.format("Send stage request for %s to TReqS.", hsmPath));
    HttpResponse<JsonNode> response;
    try {
      response = Unirest.post(treqsStaging + "/request")
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
    LOGGER.debug(String.format("TReqS created request for %s with id '%s'.", hsmPath, id));
    
    return id;
  }

  public JSONObject getStatus (String requestId) {
    LOGGER.debug(String.format("Query status for request '%s'", requestId));
    try {
      return Unirest.get(treqsStaging + "/request/" + requestId).asJson().getBody().getObject();
    } catch (UnirestException e) {
      LOGGER.error(String.format("Query status of %s failed", requestId), e);
      return null;
    }
  }

  public void cancelRecall (String hsmPath) {
    LOGGER.debug(String.format("Cancel TReqS requests for %s.", hsmPath));
    Unirest.delete(treqsStaging + "/file/" + hsmPath);
  }
  
  public void disconnect () {
    try {
      Unirest.shutdown();
    } catch (IOException e) {
      LOGGER.error("Failed to shut down Unirest connection", e);
    }
  }
}