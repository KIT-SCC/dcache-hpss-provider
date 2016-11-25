package de.gridka.dcache.nearline.hpss;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.StorageInfo;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.pool.nearline.spi.RemoveRequest;
import org.dcache.pool.nearline.spi.StageRequest;
import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Dc2HpssNearlineStorage extends ListeningNearlineStorage {
  private static final Logger LOGGER = LoggerFactory.getLogger(Dc2HpssNearlineStorage.class);

  private final String type;
  private final String name;
  private volatile String mountpoint = null;
//  private long period = 2; //minutes 
  private volatile ListeningExecutorService mover;
  private volatile ListeningExecutorService cleaner;
  private volatile ListeningScheduledExecutorService poller;

  public Dc2HpssNearlineStorage(String type, String name)
  {
      this.type = type;
      this.name = name;
  }

  protected ListeningExecutorService getMover () {
    return mover;
  }

  protected ListeningExecutorService getCleaner () {
    return cleaner;
  }

  protected ListeningScheduledExecutorService getPoller () {
    return poller;
  }
  
  @Override
  public synchronized void configure(Map<String, String> properties) throws IllegalArgumentException {
    LOGGER.trace("Configuring HSM interface '{}' with type '{}'.", name, type);

    String mnt = properties.get("mountpoint");
    if (mnt != null) {
      checkArgument(Files.isDirectory(Paths.get(mnt)), mnt + " is not a directory!");
      this.mountpoint = mnt;
      LOGGER.trace("Set mountpoint to {}.", mnt);
    } else {
      checkArgument(mountpoint != null, "mountpoint attribute is required!");
    }
    
/*
    String prd = properties.get("period");
    if (prd != null) {
      try {
        this.period = Integer.parseInt(prd);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("period is not assigned an integer number!", e);
      }
    }
*/
    
    String copies = properties.getOrDefault("copies", "5");
    try {
      this.mover = new MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(Integer.parseInt(copies)));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("copies is not assigned an integer number!", e);
    }
    
    this.cleaner = new MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    this.poller = new MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(1));
  }
  
  @Override
  public ListenableFuture<Void> remove(final RemoveRequest request) {
    return getCleaner().submit(new RemoveTask(request, mountpoint));
  }
  
  @Override
  public ListenableFuture<Set<URI>> flush(final FlushRequest request) {
    final FlushTask task = new FlushTask(type, name, request, mountpoint);
    return Futures.transform(request.activate(),
        new AsyncFunction<Void, Set<URI>> () {
          public ListenableFuture<Set<URI>> apply(Void ignored) throws CacheException, URISyntaxException {
            return getMover().submit(task);
          }
        }
    );
  }
  
  @Override
  public ListenableFuture<Set<Checksum>> stage(final StageRequest request) {
    final PreStageTask preStageTask = new PreStageTask("treqs-server", getPoller(), request);
    final StageTask stageTask = new StageTask(request, mountpoint);
    
    ListenableFuture<Void> activation = request.activate();
    AsyncFunction<Void, Void> allocation = new AsyncFunction<Void, Void> () {
      @Override
      public ListenableFuture<Void> apply(Void ignored) throws Exception {
          return request.allocate();
      }
    };
    AsyncFunction<Void, Void> prestaging = new AsyncFunction<Void, Void> () {
      @Override
      public ListenableFuture<Void> apply(Void ignored) throws Exception {
          return poller.submit(preStageTask);
      }
    };
    AsyncFunction<Void, Set<Checksum>> staging = new AsyncFunction<Void, Set<Checksum>> () {
      @Override
      public ListenableFuture<Set<Checksum>> apply(Void ignored) throws Exception {
          return mover.submit(stageTask);
      }
    };
    
    return Futures.transform(Futures.transform(Futures.transform(activation, allocation), prestaging), staging);
  }
}
