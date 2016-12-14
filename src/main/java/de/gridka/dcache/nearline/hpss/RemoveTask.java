package de.gridka.dcache.nearline.hpss;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;

import org.dcache.pool.nearline.spi.RemoveRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diskCacheV111.util.CacheException;

public class RemoveTask implements Callable<Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(Dc2HpssNearlineStorage.class);
  private Path externalPath;
  
  RemoveTask (RemoveRequest request, String mountpoint) {
    String hsmPath = request.getUri().getPath();
    this.externalPath = Paths.get(mountpoint, hsmPath);
    LOGGER.trace(String.format("Created RemoveTask for %s.", externalPath));
  }
  
  public Void call () throws CacheException {
    
    try {
      LOGGER.debug(String.format("Deleting %s.", externalPath));
      Files.delete(externalPath);
    } catch (IOException e) {
      throw new CacheException("Deletion of " + externalPath.toString() + " failed.", e);
    }
    return null;
  }

}