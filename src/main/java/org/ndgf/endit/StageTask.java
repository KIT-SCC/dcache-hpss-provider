/* dCache Endit Nearline Storage Provider
 *
 * Copyright (C) 2014-2015 Gerd Behrmann
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.ndgf.endit;

import com.google.common.base.Charsets;
import com.sun.jna.Library;
import com.sun.jna.Native;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.dcache.pool.nearline.spi.StageRequest;
import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;

import static java.util.Arrays.asList;

class StageTask implements PollingTask<Set<Checksum>>
{
    public static final int ERROR_GRACE_PERIOD = 1000;

    private static final int PID = CLibrary.INSTANCE.getpid();

    private final Path file;
    private final Path inFile;
    private final Path errorFile;
    private final Path requestFile;
    private final long size;

    StageTask(StageRequest request, Path requestDir, Path inDir)
    {
        file = request.getFile().toPath();
        FileAttributes fileAttributes = request.getFileAttributes();
        String id = fileAttributes.getPnfsId().toString();
        size = fileAttributes.getSize();
        inFile = inDir.resolve(id);
        errorFile = requestDir.resolve(id + ".err");
        requestFile = requestDir.resolve(id);
    }

    @Override
    public List<Path> getFilesToWatch()
    {
        return asList(errorFile, inFile);
    }

    @Override
    public Set<Checksum> start() throws Exception
    {
        if (Files.isRegularFile(inFile) && Files.size(inFile) == size) {
            Files.move(inFile, file, StandardCopyOption.ATOMIC_MOVE);
            return Collections.emptySet();
        }
        String s = String.format("{ \"file_size\": %d, \"parent_pid\": %d, \"time\": %d }",
                                 size, PID, System.currentTimeMillis() / 1000);
        Files.write(requestFile, s.getBytes(Charsets.UTF_8));
        return null;
    }

    @Override
    public Set<Checksum> poll() throws IOException, InterruptedException, EnditException
    {
        if (Files.exists(errorFile)) {
            List<String> lines;
            try {
                Thread.sleep(ERROR_GRACE_PERIOD);
                lines = Files.readAllLines(errorFile, Charsets.UTF_8);
            } finally {
                Files.deleteIfExists(inFile);
                Files.deleteIfExists(errorFile);
                Files.deleteIfExists(requestFile);
            }
            throw EnditException.create(lines);
        }
        if (Files.isRegularFile(inFile) && Files.size(inFile) == size) {
            Files.deleteIfExists(requestFile);
            Files.move(inFile, file, StandardCopyOption.ATOMIC_MOVE);
            return Collections.emptySet();
        }
        return null;
    }

    @Override
    public boolean abort() throws Exception
    {
        if (Files.deleteIfExists(requestFile)) {
            Files.deleteIfExists(errorFile);
            Files.deleteIfExists(inFile);
            return true;
        }
        return false;
    }

    private interface CLibrary extends Library
    {
        CLibrary INSTANCE = (CLibrary) Native.loadLibrary("c", CLibrary.class);
        int getpid();
    }
}
