package de.gridka.dcache.nearline.hpss;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.stream.Collectors;

import org.json.JSONObject;
import org.dcache.pool.nearline.spi.StageRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AbstractFuture;

import diskCacheV111.util.CacheException;

public class PreStageTask extends AbstractFuture<Void> implements Callable<Boolean> {
  private static final Logger LOGGER = LoggerFactory.getLogger(PreStageTask.class);
  TReqS2 treqs;
  private String hsmPath;
  private String requestId;
  
  PreStageTask (String type, String name, TReqS2 treqs, StageRequest request, String hpssRoot) {
    this.treqs = treqs;
    
    /* Get a list of all URIs for this file and filter them for
     * matching HSM type and name. Usually, one single URI should remain.
     */
    this.hsmPath = hpssRoot + request.getFileAttributes()
                                     .getStorageInfo()
                                     .locations()
                                     .stream()
                                     .filter(uri -> uri.getScheme().equals(type))
                                     .filter(uri -> uri.getAuthority().equals(name))
                                     .collect(Collectors.toList())
                                     .get(0)
                                     .getPath();
    LOGGER.debug("New PreStageTask to bring online {}.", request.getId(), hsmPath);
  }
  
  public void start () throws CacheException {
    this.requestId = treqs.initRecall(hsmPath);
    LOGGER.debug("Received {} from TReqS for PreStageTask of {}.", requestId, hsmPath);
  }
  
  @Override
  public synchronized Boolean call () throws Exception {
    try {
      if (!isDone()) {
        LOGGER.debug("Query status for {}.", requestId);
        JSONObject status = treqs.getStatus(requestId);
        if (status != null && status.getString("status").equals("ENDED")) {
          LOGGER.debug("Request {} has ENDED.", requestId);
          String subStatus = status.getString("sub_status");
          switch (subStatus) {
            case "FAILED":
              LOGGER.debug("Request {} has FAILED.", requestId);
              String error = status.getJSONObject("file").getString("error_message");
              throw new CacheException(30, error);
            case "CANCELLED":
              LOGGER.debug("Request {} was CANCELLED.", requestId);
              throw new CancellationException("Request was cancelled by TReqS.");
            case "SUCCEEDED":
              LOGGER.debug("Request {} was SUCCESSFUL.", requestId);
              return true;
          }
        } else {
          LOGGER.debug("Request {} is in status {}, reschedule another query.", requestId, status.getString("status"));
        }
      }
    } catch (Exception e) {
      try {
        LOGGER.debug("Cancelling PreStageTask {} for {}.", requestId, hsmPath);
        this.cancel();
      } catch (Exception suppressed) {
        e.addSuppressed(suppressed);
      }
      setException(e);
      throw e;
    }
    return false;
  }
  
  public synchronized boolean cancel () {
    if (isDone()) {
      return false;
    }
    LOGGER.debug("Order TReqS to cancel {} for {}.", requestId, hsmPath);
    treqs.cancelRecall(requestId);
    // Set this AbstractFuture to be cancelled.
    super.cancel(true);
    return true;
  }
}