package de.gridka.dcache.nearline.hpss;

import org.dcache.pool.nearline.spi.NearlineStorage;
import org.dcache.pool.nearline.spi.NearlineStorageProvider;

public class Dc2HpssNearlineStorageProvider implements NearlineStorageProvider
{
    @Override
    public String getName()
    {
        return "Dc2Hpss";
    }

    @Override
    public String getDescription()
    {
        return "Connect dCache to HPSS.";
    }

    @Override
    public NearlineStorage createNearlineStorage(String type, String name)
    {
        return new Dc2HpssNearlineStorage(type, name);
    }
}
