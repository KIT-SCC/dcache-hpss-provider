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
  private volatile String hpssRoot = "/";
  // A default 120 seconds delay for scheduled tasks on poller.
  private volatile int period = 120;
  private TReqS2 treqs = null;
  private volatile ListeningExecutorService mover = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(5));
  private volatile ListeningExecutorService cleaner = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
  private volatile ListeningScheduledExecutorService poller = MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(1));

  public Dc2HpssNearlineStorage(String type, String name)
  {
      this.type = type;
      this.name = name;
  }
  
  @Override
  public synchronized void configure (Map<String, String> properties) throws IllegalArgumentException {
    LOGGER.debug("Configuring HSM interface '{}' with type '{}'.", name, type);
    
    String treqsHost = null;
    String treqsPort = "8080";
    String treqsUser = "treqs";
    String treqsPassword = "changeit";
    
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      switch (entry.getKey()) {
        case "mountpoint":
          checkArgument(Files.isDirectory(Paths.get(entry.getValue())), entry.getValue() + " is not a directory!");
          this.mountpoint = entry.getValue();
          LOGGER.debug("Set mountpoint to {}.", mountpoint);
          break;
        case "treqsHost":
          treqsHost = entry.getValue();
          break;
        case "treqsPort":
          treqsPort = entry.getValue();
          break;
        case "treqsUser":
          treqsUser = entry.getValue();
          break;
        case "treqsPassword":
          treqsPassword = entry.getValue();
          break;
        case "hpssRoot":
          this.hpssRoot = entry.getValue();
          break;
        case "copies":
          try {
            this.mover = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(Integer.parseInt(entry.getValue())));
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException("copies is not assigned an integer number!", e);
          }
          break;
        case "period":
          try {
            this.period = Integer.parseInt(entry.getValue());
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException("period is not assigned an integer number!", e);
          }
          break;
        default:
          LOGGER.warn("Property '{}' has no effect on HSM interface '{}'.", entry.getKey(), name);
      }
    }
    
    checkArgument(mountpoint != null, "mountpoint attribute is required!");

    if (treqsHost != null) {
      this.treqs = new TReqS2(treqsHost, treqsPort, treqsUser, treqsPassword);
      LOGGER.debug("Created TReqS server {}.", treqsHost);
    } else {
      checkArgument(treqs != null, "treqsHost property is required!");
    }
  }
  
  @Override
  public ListenableFuture<Void> remove (final RemoveRequest request) {
    return cleaner.submit(new RemoveTask(request, mountpoint));
  }
  
  @Override
  public ListenableFuture<Set<URI>> flush (final FlushRequest request) {
    final FlushTask task = new FlushTask(type, name, request, mountpoint);
    return Futures.transform(request.activate(),
        new AsyncFunction<Void, Set<URI>> () {
          public ListenableFuture<Set<URI>> apply (Void ignored) throws CacheException, URISyntaxException {
            return mover.submit(task);
          }
        }
    );
  }
  
  @Override
  public ListenableFuture<Set<Checksum>> stage (final StageRequest request) {
    String pnfsId = request.getFile().getName();
    // Signal the activation of the StageRequest to dCache.
    ListenableFuture<Void> activation = request.activate();
    
    // Asynchronously allocate the disk space for the staging in dCache. 
    AsyncFunction<Void, Void> allocation = new AsyncFunction<Void, Void> () {
      @Override
      public ListenableFuture<Void> apply (Void ignored) throws Exception {
        return request.allocate();
      }
    };
    
    PreStageTask task = new PreStageTask(type, name, treqs, request, hpssRoot);

    // Check on the PreStageTask - when it is completed, return immediately,
    // otherwise schedule another recheck.
    AsyncFunction<Boolean, Void> recheck = new AsyncFunction<Boolean, Void> () {
      @Override
      public ListenableFuture<Void> apply (Boolean completed) {
        if (completed) {
          LOGGER.debug("Pre-staging completed for {}", pnfsId);
          return Futures.immediateFuture(null);
        } else if (task.isCancelled()) {
          LOGGER.debug("Pre-staging for {} has been cancelled.", pnfsId);
          return Futures.immediateCancelledFuture();
        } else {
          LOGGER.debug("Rescheduling pre-stage request for {}", pnfsId);
          return Futures.transform(poller.schedule(task, period, TimeUnit.SECONDS), this);
        }
      }
    };
    
    // Encapsulate the start and subsequent checks on the PreStageTask,
    // so we can chain staging after it.
    AsyncFunction<Void, Void> prestaging = new AsyncFunction<Void, Void> () {
      @Override
      public ListenableFuture<Void> apply (Void ignored) throws CacheException {
        LOGGER.debug("Submitting pre-stage request for {}", pnfsId);
        task.start();
        return Futures.transform(poller.schedule(task, period, TimeUnit.SECONDS), recheck);
      }
    };
    
    // Once the pre-staging is done, stage the file into the pool's inventory.
    AsyncFunction<Void, Set<Checksum>> staging = new AsyncFunction<Void, Set<Checksum>> () {
      @Override
      public ListenableFuture<Set<Checksum>> apply (Void ignored) throws Exception {
        LOGGER.debug("Submitting stage request for {}", pnfsId);
        return mover.submit(new StageTask(type, name, request, mountpoint));          
      }
    };
    
    return Futures.transform(Futures.transform(Futures.transform(activation, prestaging), allocation), staging);
  }
  
  @Override
  public void shutdown () {
    treqs.disconnect();
    mover.shutdown();
    poller.shutdown();
    cleaner.shutdown();
  }
}
