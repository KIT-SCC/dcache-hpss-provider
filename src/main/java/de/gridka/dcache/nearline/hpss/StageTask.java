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
    
    String hsmPath = String.format("/%s/%s/%s/%s",
        fileAttributes.getStorageInfo().getKey("group"),
        pnfsId.charAt(pnfsId.length() - 1),
        pnfsId.charAt(pnfsId.length() - 2),
        pnfsId
      );
    this.externalPath = Paths.get(mountpoint, hsmPath);
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