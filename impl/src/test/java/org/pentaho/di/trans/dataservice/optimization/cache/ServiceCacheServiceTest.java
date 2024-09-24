/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2024 by Hitachi Vantara : http://www.pentaho.com
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
