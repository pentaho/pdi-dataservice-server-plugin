package org.pentaho.di.trans.dataservice;

import org.pentaho.caching.api.PentahoCacheManager;
import org.pentaho.caching.api.PentahoCacheSystemConfiguration;
import org.pentaho.caching.PentahoCacheManagerImpl;
import org.pentaho.caching.ri.HeapCacheProvidingService;

public class DataServiceCacheManagerService {

    private static PentahoCacheManager cacheManager;

    public static PentahoCacheManager getInstance() {
        if ( cacheManager == null ) {
            PentahoCacheSystemConfiguration pentahoCacheSystemConfiguration = new PentahoCacheSystemConfiguration();
            cacheManager = new PentahoCacheManagerImpl( pentahoCacheSystemConfiguration, new HeapCacheProvidingService( ) );
        }
        return cacheManager;
    }
}
