package de.gridka.dcache.nearline.hpss;

import java.util.concurrent.TimeUnit;

import org.dcache.pool.nearline.spi.StageRequest;
import org.dcache.vehicles.FileAttributes;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

public class PreStageTask extends AbstractFuture<Void> implements Runnable {
  String treqs;
  ListeningScheduledExecutorService poller;
  private ListenableScheduledFuture<?> future;
  private String hsmPath;
  private String requestId;
  
  PreStageTask (String treqs, ListeningScheduledExecutorService poller, StageRequest request) {
    this.treqs = treqs;
    this.poller = poller;
    
    FileAttributes fileAttributes = request.getFileAttributes();
    String pnfsId = fileAttributes.getPnfsId().toString();
    StringBuilder sb = new StringBuilder();
    sb.append('/' + fileAttributes.getStorageInfo().getKey("store"));
    sb.append('/' + pnfsId.substring(0, 5));
    sb.append('/' + pnfsId.charAt(5));
    sb.append('/' + pnfsId);
    this.hsmPath = sb.toString();

    this.requestId = treqs + " requested";
  }
  
  public synchronized void run() {
    try {
      if (!isDone()) {
          Boolean result = treqs.contains(hsmPath);
          if (result) {
              set(null);
          } else {
              future = poller.schedule(this, 2, TimeUnit.MINUTES);
          }
      }
    } catch (Exception e) {
        setException(e);
    }
  }
  
  public synchronized boolean cancel () {
    if (isDone()) {
      return false;
    }
    treqs = requestId + "canceled";
    future.cancel(true);
    return true;
  }
}