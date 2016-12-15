package de.gridka.dcache.nearline.hpss;

import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

import javax.json.JsonObject;

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
    LOGGER.trace(String.format("Create new PreStageTask for %s.", request.toString()));
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

    LOGGER.debug(String.format("PreStageTask for %s has to bring online %s.", request.toString(), hsmPath));
    this.requestId = treqs.initRecall(hsmPath);
    LOGGER.debug(String.format("Received %s for PreStageTask of %s", requestId, hsmPath));
  }
  
  public synchronized void run() {
    try {
      if (!isDone()) {
        LOGGER.debug(String.format("Query status for %s.", requestId));
        JsonObject status = treqs.getStatus(requestId);
        if (status.getString("status") == "ENDED") {
          LOGGER.debug(String.format("Request %s has ENDED.", requestId));
          if (status.getString("substatus") == "FAILED") {
            LOGGER.debug(String.format("Request %s has FAILED.", requestId));
            String error = status.getJsonObject("file").getString("error_message");
            throw new CacheException(30, error);
          } else if (status.getString("substatus") == "CANCELLED") {
            LOGGER.debug(String.format("Request %s was CANCELLED.", requestId));
            throw new CancellationException("Request was cancelled by TReqS.");
          } else if (status.getString("substatus") == "SUCCEEDED") {
            LOGGER.debug(String.format("Request %s was SUCCESSFUL.", requestId));
            set(null);
          }
        } else {
          LOGGER.debug(String.format("Request %s is rescheduled.", requestId));
          future = poller.schedule(this, 2, TimeUnit.MINUTES);
        }
      }
    } catch (Exception e) {
      try {
        LOGGER.debug(String.format("Cancelling PreStageTask for %s.", hsmPath));
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
    LOGGER.debug(String.format("Order TReqQs to cancel %s for %s.", requestId, hsmPath));
    treqs.cancelRecall(hsmPath);
    future.cancel(true);
    return true;
  }
}