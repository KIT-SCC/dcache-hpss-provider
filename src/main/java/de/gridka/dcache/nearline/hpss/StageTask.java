package de.gridka.dcache.nearline.hpss;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.dcache.pool.nearline.spi.StageRequest;
import org.dcache.util.Checksum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diskCacheV111.util.CacheException;

class StageTask implements Callable<Set<Checksum>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(StageTask.class);
  private Path path;
  private Path externalPath;
  
  public StageTask (String type, String name, StageRequest request, String mountpoint) {
    LOGGER.debug("Create new StageTask for {}.", request.toString());
    
    this.path = request.getFile().toPath();
    /* Get a list of all URIs for this file and filter them for
     * matching HSM type and name. Usually, one single URI should remain.
     */
    String hsmPath = request.getFileAttributes()
                            .getStorageInfo()
                            .locations()
                            .stream()
                            .filter(uri -> uri.getScheme().equals(type))
                            .filter(uri -> uri.getAuthority().equals(name))
                            .collect(Collectors.toList())
                            .get(0)
                            .getPath();
    this.externalPath = Paths.get(mountpoint, hsmPath);
    LOGGER.debug("StageTask {} has to copy {} to {}.", request.toString(), externalPath, path);
  }
  
  public Set<Checksum> call () throws CacheException {
    try {
      LOGGER.debug("Copy {} to {}.", externalPath, path);
      Files.copy(externalPath, path);
    } catch (IOException e) {
      throw new CacheException(2, "Copy to " + externalPath.toString() + " failed.", e);
    }
    
    return Collections.emptySet();
  }
}