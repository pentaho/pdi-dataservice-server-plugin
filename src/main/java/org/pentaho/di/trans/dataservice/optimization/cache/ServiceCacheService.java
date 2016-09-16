/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

import org.pentaho.caching.api.PentahoCacheManager;

import javax.cache.Cache;

/**
 * Created by bmorrise on 9/15/16.
 */
public class ServiceCacheService implements CacheService {

  private PentahoCacheManager cacheManager;

  public ServiceCacheService( PentahoCacheManager cacheManager ) {
    this.cacheManager = cacheManager;
  }

  public Cache getCache( String dataServiceName ) {
    return cacheManager.getCache( getCacheName( dataServiceName ), CachedService.CacheKey.class, CachedService.class );
  }

  public void destroyCache( String dataServiceName ) {
    cacheManager.destroyCache( getCacheName( dataServiceName ) );
  }

  public String getCacheName( String dataServiceName ) {
    return "SERVICE_CACHE." + dataServiceName;
  }
}
