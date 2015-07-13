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

package org.pentaho.di.trans.dataservice.cache;

import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.caching.api.Constants;
import org.pentaho.caching.api.PentahoCacheManager;
import org.pentaho.di.trans.TransMeta;

import javax.cache.Cache;

public class DataServiceMetaCache {

  private PentahoCacheManager cacheManager;

  public DataServiceMetaCache( PentahoCacheManager cacheManager ) {
    this.cacheManager = cacheManager;
  }

  public DataServiceMeta get( TransMeta transMeta, String stepName ) {
    DataServiceMeta.CacheKey key = DataServiceMeta.createCacheKey( transMeta, stepName );
    return getCache().get( key );
  }

  public void put( TransMeta transMeta, String stepName, DataServiceMeta dataService ) {
    DataServiceMeta.CacheKey key = DataServiceMeta.createCacheKey( transMeta, stepName );
    getCache().put( key, dataService != null ? dataService : new DataServiceMeta() );
  }

  private Cache<DataServiceMeta.CacheKey, DataServiceMeta> getCache() {
    String cacheName = DataServiceMetaCache.class.getName();
    Cache<DataServiceMeta.CacheKey, DataServiceMeta> cache;
    cache = cacheManager.getCache( cacheName, DataServiceMeta.CacheKey.class, DataServiceMeta.class );
    if ( cache == null ) {
      cache = cacheManager.getTemplates().get( Constants.DEFAULT_TEMPLATE )
        .createCache( cacheName, DataServiceMeta.CacheKey.class, DataServiceMeta.class );
    }
    return cache;
  }

}
