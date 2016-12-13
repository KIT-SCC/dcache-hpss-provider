package de.gridka.dcache.nearline.hpss;

import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

import javax.json.JsonObject;

import org.dcache.pool.nearline.spi.StageRequest;
import org.dcache.vehicles.FileAttributes;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import diskCacheV111.util.CacheException;

public class PreStageTask extends AbstractFuture<Void> implements Runnable {
  TReqS2 treqs;
  ListeningScheduledExecutorService poller;
  private ListenableScheduledFuture<?> future;
  private String hsmPath;
  private String requestId;
  
  PreStageTask (TReqS2 treqs, ListeningScheduledExecutorService poller, StageRequest request) {
    this.treqs = treqs;
    this.poller = poller;
    
    FileAttributes fileAttributes = request.getFileAttributes();
    String pnfsId = fileAttributes.getPnfsId().toString();
    StringBuilder sb = new StringBuilder();
    sb.append('/' + fileAttributes.getStorageInfo().getKey("group"));
    sb.append('/' + pnfsId.substring(0, 5));
    sb.append('/' + pnfsId.charAt(5));
    sb.append('/' + pnfsId);
    this.hsmPath = sb.toString();

    this.requestId = treqs.initRecall(hsmPath);
  }
  
  public synchronized void run() {
    try {
      if (!isDone()) {
        JsonObject status = treqs.getStatus(requestId);
        if (status.getString("status") == "ENDED") {
          if (status.getString("substatus") == "FAILED") {
            String error = status.getJsonObject("file").getString("error_message");
            throw new CacheException(30, error);
          } else if (status.getString("substatus") == "CANCELLED") {
            throw new CancellationException("Request was cancelled by TReqS.");
          } else if (status.getString("substatus") == "SUCCEEDED") {
            set(null);
          }
        } else {
            future = poller.schedule(this, 2, TimeUnit.MINUTES);
        }
      }
    } catch (Exception e) {
      try {
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
    treqs.cancelRecall(hsmPath);
    future.cancel(true);
    return true;
  }
}