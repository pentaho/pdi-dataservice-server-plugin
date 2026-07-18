/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
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
