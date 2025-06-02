/*!
 * Copyright 2010 - 2018 Hitachi Vantara.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.pentaho.caching;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.pentaho.caching.api.Constants;
import org.pentaho.caching.api.PentahoCacheProvidingService;
import org.pentaho.caching.ehcache.EhcacheProvidingService;
import org.pentaho.caching.ri.HeapCacheProvidingService;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author nhudak
 */
public class PentahoCacheManagerFactory {

    private final ListMultimap<String, SettableFuture<PentahoCacheProvidingService>> providerMap;
    Map<String, String> properties = new HashMap<>();
    public PentahoCacheManagerFactory(  ) {
        providerMap = LinkedListMultimap.create();
    }

    public void init() {
            PentahoCacheProvidingService ehcacheService = new EhcacheProvidingService();
            PentahoCacheProvidingService guavaService = new HeapCacheProvidingService();

            registerProvider(Constants.PENTAHO_CACHE_PROVIDER_EHCACHE, ehcacheService );
            registerProvider(Constants.PENTAHO_CACHE_PROVIDER_GUAVA, ehcacheService );
    }

    protected synchronized ListenableFuture<PentahoCacheProvidingService> getProviderService( String providerId ) {
        List<SettableFuture<PentahoCacheProvidingService>> futureList = providerMap.get( providerId );
        if ( futureList.isEmpty() ) {
            futureList.add( SettableFuture.<PentahoCacheProvidingService>create() );
        }
        return futureList.get( 0 );
    }

    public synchronized void registerProvider( String id, PentahoCacheProvidingService provider ) {
        List<SettableFuture<PentahoCacheProvidingService>> futureList = providerMap.get( id );
        if ( futureList.isEmpty() ) {
            futureList.add( SettableFuture.<PentahoCacheProvidingService>create() );
        }
        for ( SettableFuture<PentahoCacheProvidingService> future : futureList ) {
            future.set( provider );
        }
    }

    public synchronized void unregisterProvider( String providerId, PentahoCacheProvidingService provider ) {
        Set<ListenableFuture<PentahoCacheProvidingService>> invalidFutures = Sets.newHashSet();
        for ( Iterator<SettableFuture<PentahoCacheProvidingService>> iterator = providerMap.get( providerId ).iterator();
              iterator.hasNext(); ) {
            SettableFuture<PentahoCacheProvidingService> future = iterator.next();
            try {
                if ( future.isDone() && future.get( 10, TimeUnit.SECONDS ).equals( provider ) ) {
                    iterator.remove();
                    invalidFutures.add( future );
                }
            } catch ( Throwable t ) {
                Logger.getLogger( providerId ).log( Level.WARNING, "Unexpected exception", t );
            }
        }
    }

}
