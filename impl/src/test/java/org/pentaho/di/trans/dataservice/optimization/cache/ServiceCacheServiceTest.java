/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package org.pentaho.di.trans.dataservice.optimization.cache;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.caching.api.PentahoCacheManager;

import javax.cache.Cache;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bmorrise on 9/15/16.
 */
@RunWith( MockitoJUnitRunner.StrictStubs.class)
public class ServiceCacheServiceTest {

  @Mock PentahoCacheManager cacheManager;
  @Mock Cache cache;

  private ServiceCacheService serviceCacheService;
  private String DATA_SERVICE_NAME = "cachedDataService";

  @Before
  public void setup() {
    serviceCacheService = new ServiceCacheService( cacheManager );
  }

  @Test
  public void testGetCache() {
    when( cacheManager.getCache( serviceCacheService.getCacheName( DATA_SERVICE_NAME ), CachedService.CacheKey.class,
      CachedService.class ) ).thenReturn( cache );

    Cache cache = serviceCacheService.getCache( DATA_SERVICE_NAME );

    assertNotNull( cache );
  }

  @Test
  public void testDestoryCache() {
    serviceCacheService.destroyCache( DATA_SERVICE_NAME );

    verify( cacheManager, times( 1 ) ).destroyCache( serviceCacheService.getCacheName( DATA_SERVICE_NAME ) );
  }

}
