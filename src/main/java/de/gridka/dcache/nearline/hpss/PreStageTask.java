package de.gridka.dcache.nearline.hpss;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;

import org.json.JSONObject;
import org.dcache.pool.nearline.spi.StageRequest;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;

import diskCacheV111.util.CacheException;

public class PreStageTask extends AbstractFuture<Void> implements Callable<Boolean> {
  private static final Logger LOGGER = LoggerFactory.getLogger(Dc2HpssNearlineStorage.class);
  TReqS2 treqs;
  private ListenableScheduledFuture<?> future;
  private String hsmPath;
  private String requestId;
  
  PreStageTask (TReqS2 treqs, StageRequest request) throws CacheException {
    LOGGER.debug("Create new PreStageTask for {}.", request.toString());
    this.treqs = treqs;
    
    FileAttributes fileAttributes = request.getFileAttributes();
    String pnfsId = fileAttributes.getPnfsId().toString();
    this.hsmPath = String.format("/%s/%s/%s/%s",
        fileAttributes.getStorageInfo().getKey("group"),
        pnfsId.charAt(pnfsId.length() - 1),
        pnfsId.charAt(pnfsId.length() - 2),
        pnfsId
      );

    LOGGER.debug("PreStageTask for {} has to bring online {}.", request.toString(), hsmPath);
    this.requestId = treqs.initRecall(hsmPath);
    LOGGER.debug("Received {} for PreStageTask of {}", requestId, hsmPath);
  }
  
  public synchronized Boolean call () {
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
          LOGGER.debug("Request {} is in status {} and will be rescheduled.", requestId, status.getString("status"));
          return false;
        }
      }
    } catch (Exception e) {
      try {
        LOGGER.debug("Cancelling PreStageTask for {}.", hsmPath);
        this.cancel();
      } catch (Exception suppressed) {
        e.addSuppressed(suppressed);
      }
      setException(e);
    }
    return false;
  }
  
  public synchronized boolean cancel () {
    if (isDone()) {
      return false;
    }
    LOGGER.debug("Order TReqS to cancel {} for {}.", requestId, hsmPath);
    treqs.cancelRecall(requestId);
    future.cancel(true);
    return true;
  }
}