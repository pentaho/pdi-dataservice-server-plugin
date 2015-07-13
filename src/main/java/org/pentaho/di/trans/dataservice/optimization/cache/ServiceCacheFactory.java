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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptTypeForm;
import org.pentaho.caching.api.PentahoCacheManager;
import org.pentaho.caching.api.PentahoCacheTemplateConfiguration;

import javax.cache.Cache;
import javax.cache.CacheException;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * @author nhudak
 */
public class ServiceCacheFactory implements PushDownFactory {
  public static final String CACHE_NAME = ServiceCache.class.getName();
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

  @Override public PushDownOptTypeForm createPushDownOptTypeForm() {
    return new ServiceCacheOptForm( this );
  }

  public ListeningExecutorService getExecutorService() {
    return executorService;
  }

  public Cache<CachedService.CacheKey, CachedService> getCache( ServiceCache serviceCache )
    throws CacheException {
    String templateName = serviceCache.getTemplateName();
    Map<String, PentahoCacheTemplateConfiguration> templates = cacheManager.getTemplates();

    Cache<CachedService.CacheKey, CachedService> cache = cacheManager.getCache(
      CACHE_NAME,
      CachedService.CacheKey.class,
      CachedService.class
    );

    if ( cache != null ) {
      return cache;
    } else {
      Preconditions.checkState( !Strings.isNullOrEmpty( templateName ),
        "Cache Template is not configured" );
      Preconditions.checkState( templates.containsKey( templateName ),
        "Cache Template is not configured", templateName );

      return templates.get( templateName ).createCache(
        CACHE_NAME,
        CachedService.CacheKey.class,
        CachedService.class
      );
    }
  }

  public ServiceObserver createObserver( DataServiceExecutor executor ) {
    return new ServiceObserver( executor );
  }

  public CachedServiceLoader createCachedServiceLoader( CachedService cachedService ) {
    return new CachedServiceLoader( cachedService, executorService );
  }

  public Iterable<String> getTemplates() {
    // TODO need better template filtering & selection
    return cacheManager.getTemplates().keySet();
  }
}
