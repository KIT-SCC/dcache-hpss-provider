package de.gridka.dcache.nearline.hpss;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Callable;

import org.dcache.pool.nearline.spi.StageRequest;
import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;

import diskCacheV111.util.CacheException;

class StageTask implements Callable<Set<Checksum>> {
  private Path path;
  private Path externalPath;
  
  public StageTask(StageRequest request, String mountpoint) {
    FileAttributes fileAttributes = request.getFileAttributes();
    String pnfsId = fileAttributes.getPnfsId().toString();
    this.path = request.getFile().toPath();
    
    StringBuilder sb = new StringBuilder();
    sb.append('/' + fileAttributes.getStorageInfo().getKey("store"));
    sb.append('/' + pnfsId.substring(0, 5));
    sb.append('/' + pnfsId.charAt(5));
    sb.append('/' + pnfsId);
    this.externalPath = Paths.get(mountpoint, sb.toString());
  }
  
  public Set<Checksum> call () throws CacheException {
    try {
      Files.copy(externalPath, path);
    } catch (IOException e) {
      throw new CacheException(2, "Copy to " + externalPath.toString() + " failed.", e);
    }
    
    return Collections.emptySet();
  }
}