/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2015 Pentaho Corporation (Pentaho). All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Pentaho and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Pentaho and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Pentaho is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Pentaho,
 * explicitly covering such access.
 */

package com.pentaho.di.trans.dataservice.cache;

import com.pentaho.di.trans.dataservice.DataServiceMeta;
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
