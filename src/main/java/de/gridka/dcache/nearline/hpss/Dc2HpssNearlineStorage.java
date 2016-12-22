package de.gridka.dcache.nearline.hpss;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import diskCacheV111.util.CacheException;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.pool.nearline.spi.RemoveRequest;
import org.dcache.pool.nearline.spi.StageRequest;
import org.dcache.util.Checksum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Dc2HpssNearlineStorage extends ListeningNearlineStorage {
  private static final Logger LOGGER = LoggerFactory.getLogger(Dc2HpssNearlineStorage.class);

  private final String type;
  private final String name;
  private volatile String mountpoint = null;
  private TReqS2 treqs = null;
  private volatile ListeningExecutorService mover;
  private volatile ListeningExecutorService cleaner;
  private volatile ListeningScheduledExecutorService poller;
  private int period;

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
  public synchronized void configure (Map<String, String> properties) throws IllegalArgumentException {
    LOGGER.debug("Configuring HSM interface '{}' with type '{}'.", name, type);

    String mnt = properties.get("mountpoint");
    if (mnt != null) {
      checkArgument(Files.isDirectory(Paths.get(mnt)), mnt + " is not a directory!");
      this.mountpoint = mnt;
      LOGGER.debug("Set mountpoint to {}.", mnt);
    } else {
      checkArgument(mountpoint != null, "mountpoint attribute is required!");
    }
    
    String treqsHost = properties.get("treqsHost");
    String treqsPort = properties.getOrDefault("treqsPort", "8080");
    String treqsUser = properties.getOrDefault("treqsUser", "treqs");
    String treqsPassword = properties.getOrDefault("treqsPassword", "changeit");
    if (treqsHost != null) {
      this.treqs = new TReqS2(treqsHost, treqsPort, treqsUser, treqsPassword);
      LOGGER.debug("Created TReqS server {}.", treqsHost);
    } else {
      checkArgument(treqs != null, "treqsHost attribute is required!");
    }
      
    String copies = properties.getOrDefault("copies", "5");
    try {
      this.mover = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(Integer.parseInt(copies)));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("copies is not assigned an integer number!", e);
    }
    
    // A default 2 minute delay for scheduled tasks on poller.
    String delay = properties.getOrDefault("period", "2");
    try {
      this.period = Integer.parseInt(delay);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("period is not assigned an integer number!", e);
    }
    
    this.cleaner = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    this.poller = MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(1));
  }
  
  @Override
  public ListenableFuture<Void> remove (final RemoveRequest request) {
    return getCleaner().submit(new RemoveTask(request, mountpoint));
  }
  
  @Override
  public ListenableFuture<Set<URI>> flush (final FlushRequest request) {
    final FlushTask task = new FlushTask(type, name, request, mountpoint);
    return Futures.transform(request.activate(),
        new AsyncFunction<Void, Set<URI>> () {
          public ListenableFuture<Set<URI>> apply (Void ignored) throws CacheException, URISyntaxException {
            return getMover().submit(task);
          }
        }
    );
  }
  
  @Override
  public ListenableFuture<Set<Checksum>> stage (final StageRequest request) {
    LOGGER.debug("Activating request {}", request.toString());
    ListenableFuture<Void> activation = request.activate();
    PreStageTask task;
    try {
      task = new PreStageTask(treqs, request);
    } catch (CacheException e) {
      LOGGER.error("Creating the PreStageTask for " + request + " failed.", e);
      return Futures.immediateFailedFuture(e);
    }
    
    AsyncFunction<Void, Void> allocation = new AsyncFunction<Void, Void> () {
      @Override
      public ListenableFuture<Void> apply (Void ignored) throws Exception {
        LOGGER.debug("Allocating space for {}", request.toString());
        return request.allocate();
      }
    };
    
    AsyncFunction<Boolean, Void> recheck = new AsyncFunction<Boolean, Void> () {
      @Override
      public ListenableFuture<Void> apply (Boolean completed) {
        if (completed) {
          LOGGER.debug("Pre-staging completed for {}", request.toString());
          return Futures.immediateFuture(null);
        } else {
          LOGGER.debug("Rescheduling pre-stage request for {}", request.toString());
          return Futures.transform(getPoller().schedule(task, period, TimeUnit.MINUTES), this);
        }
      }
    };
    
    AsyncFunction<Void, Void> prestaging = new AsyncFunction<Void, Void> () {
      @Override
      public ListenableFuture<Void> apply (Void ignored) throws CacheException {
        LOGGER.debug("Submitting pre-stage request for {}", request.toString());
        task.call();
        return Futures.transform(getPoller().submit(task), recheck);
      }
    };
    
    AsyncFunction<Void, Set<Checksum>> staging = new AsyncFunction<Void, Set<Checksum>> () {
      @Override
      public ListenableFuture<Set<Checksum>> apply (Void ignored) throws Exception {
        LOGGER.debug("Submitting stage request for {}", request.toString());
        return getMover().submit(new StageTask(request, mountpoint));
      }
    };
    
    return Futures.transform(Futures.transform(Futures.transform(activation, allocation), prestaging), staging);
  }
  
  @Override
  public void shutdown () {
    treqs.disconnect();
    mover.shutdown();
    poller.shutdown();
    cleaner.shutdown();
  }
}
