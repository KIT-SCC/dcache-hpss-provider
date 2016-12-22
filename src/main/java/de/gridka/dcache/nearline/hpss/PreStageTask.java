package de.gridka.dcache.nearline.hpss;

import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

import org.json.JSONObject;

import org.dcache.pool.nearline.spi.StageRequest;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import diskCacheV111.util.CacheException;

public class PreStageTask extends AbstractFuture<Void> implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(Dc2HpssNearlineStorage.class);
  TReqS2 treqs;
  ListeningScheduledExecutorService poller;
  private ListenableScheduledFuture<?> future;
  private String hsmPath;
  private String requestId;
  
  PreStageTask (TReqS2 treqs, ListeningScheduledExecutorService poller, StageRequest request) throws CacheException {
    LOGGER.debug("Create new PreStageTask for {}.", request.toString());
    this.treqs = treqs;
    this.poller = poller;
    
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
    LOGGER.debug("Received %s for PreStageTask of {}", requestId, hsmPath);
  }
  
  public synchronized void run() {
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
              set(null);
          }
        } else {
          LOGGER.debug("Request {} is in status{} and will be rescheduled.", requestId, status.getString("status"));
          future = poller.schedule(this, 2, TimeUnit.MINUTES);
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
  }
  
  public synchronized boolean cancel () {
    if (isDone()) {
      return false;
    }
    LOGGER.debug("Order TReqQs to cancel {} for {}.", requestId, hsmPath);
    treqs.cancelRecall(hsmPath);
    future.cancel(true);
    return true;
  }
}