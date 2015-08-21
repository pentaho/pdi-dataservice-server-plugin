/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.dataservice.optimization.cache;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.pentaho.caching.api.PentahoCacheManager;
import org.pentaho.caching.api.PentahoCacheTemplateConfiguration;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.pentaho.di.trans.dataservice.optimization.cache.ui.ServiceCacheOverlay;
import org.pentaho.di.trans.dataservice.ui.DataServiceDialog;

import javax.cache.Cache;
import javax.cache.CacheException;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkState;

/**
 * @author nhudak
 */
public class ServiceCacheFactory implements PushDownFactory {
  private final PentahoCacheManager cacheManager;

  private final ListeningExecutorService executorService;

  public ServiceCacheFactory( PentahoCacheManager cacheManager, ExecutorService executorService ) {
    this.cacheManager = cacheManager;
    this.executorService = MoreExecutors.listeningDecorator( executorService );
  }

  @Override public String getName() {
    return ServiceCache.NAME;
  }

  @Override public Class<ServiceCache> getType() {
    return ServiceCache.class;
  }

  @Override public ServiceCache createPushDown() {
    return new ServiceCache( this );
  }

  @Override public DataServiceDialog.OptimizationOverlay createOverlay() {
    return new ServiceCacheOverlay( this );
  }

  public ListeningExecutorService getExecutorService() {
    return executorService;
  }

  public PentahoCacheManager getCacheManager() {
    return cacheManager;
  }

  public Cache<CachedService.CacheKey, CachedService> getCache( ServiceCache serviceCache, String dataServiceName )
    throws CacheException {
    Optional<Cache<CachedService.CacheKey, CachedService>> cache = getCache( dataServiceName );
    if ( cache.isPresent() ) {
      return cache.get();
    }

    String templateName = serviceCache.getTemplateName();
    Map<String, PentahoCacheTemplateConfiguration> templates = cacheManager.getTemplates();
    checkState( !Strings.isNullOrEmpty( templateName ) && templates.containsKey( templateName ),
        "Cache Template is invalid", templateName );

    return templates.get( templateName )
        .overrideProperties( serviceCache.getTemplateOverrides() )
        .createCache(
            cacheName( dataServiceName ),
            CachedService.CacheKey.class,
            CachedService.class );
  }

  public Optional<Cache<CachedService.CacheKey, CachedService>> getCache( String dataServiceName ) {
    try {
      return Optional.fromNullable( cacheManager.getCache(
        cacheName( dataServiceName ),
        CachedService.CacheKey.class,
        CachedService.class
      ) );
    } catch ( Exception e ) {
      cacheManager.destroyCache( cacheName( dataServiceName ) );
      return Optional.absent();
    }
  }

  public String cacheName( String dataServiceName ) {
    return "SERVICE_CACHE." + dataServiceName;
  }

  public ServiceObserver createObserver( DataServiceExecutor executor ) {
    return new ServiceObserver( executor );
  }

  public CachedServiceLoader createCachedServiceLoader( CachedService cachedService ) {
    return new CachedServiceLoader( cachedService, executorService );
  }

  public Iterable<String> getTemplateNames() {

    // TODO need better template filtering & selection
    return cacheManager.getTemplates().keySet();
  }

  public Map<String, String> getPropertiesByTemplateName( String templateName ) {

    return cacheManager.getTemplates().get( templateName ).getProperties();
  }

}
