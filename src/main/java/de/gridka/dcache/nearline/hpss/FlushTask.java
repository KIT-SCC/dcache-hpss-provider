package de.gridka.dcache.nearline.hpss;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Callable;

import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diskCacheV111.util.CacheException;

class FlushTask implements Callable<Set<URI>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(FlushTask.class);
  private String type;
  private String name;
  private Path path;
  private Path externalPath;
  private String hsmPath;
  
  public FlushTask(String type, String name, FlushRequest request, String mountpoint) {
    LOGGER.trace(String.format("Create new FlushTask for %s.", request.toString()));
    this.type = type;
    this.name = name;
    
    FileAttributes fileAttributes = request.getFileAttributes();
    String pnfsId = fileAttributes.getPnfsId().toString();
    this.path = request.getFile().toPath();
    
    this.hsmPath = String.format("/%s/%s/%s/%s",
      fileAttributes.getStorageInfo().getKey("group"),
      pnfsId.charAt(pnfsId.length() - 1),
      pnfsId.charAt(pnfsId.length() - 2),
      pnfsId
    );
    this.externalPath = Paths.get(mountpoint, hsmPath);
    LOGGER.trace("FlushTask {} has to copy {} to {}.", request.toString(), path, externalPath);
  }
  
  public Set<URI> call () throws CacheException, URISyntaxException {
    try {
      LOGGER.debug("Copy {} to {}.", path, externalPath);
      Files.copy(path, externalPath, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      throw new CacheException(2, "Copy to " + externalPath.toString() + " failed.", e);
    }
    
    URI uri = new URI(type, name, hsmPath, null, null);
    LOGGER.debug("Return after successful copy of {} to {}: {}", path, externalPath, uri.toString());
    return Collections.singleton(uri);
  }
}