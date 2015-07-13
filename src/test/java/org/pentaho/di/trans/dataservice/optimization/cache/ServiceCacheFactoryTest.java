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

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.caching.api.PentahoCacheManager;
import org.pentaho.caching.api.PentahoCacheTemplateConfiguration;

import javax.cache.Cache;

import java.util.concurrent.ExecutorService;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */
@RunWith( MockitoJUnitRunner.class )
public class ServiceCacheFactoryTest {

  public static final String TEMPLATE_NAME = "template";
  @Mock PentahoCacheManager cacheManager;
  @Mock ExecutorService executorService;
  @InjectMocks ServiceCacheFactory serviceCacheFactory;
  @Mock Cache<CachedService.CacheKey, CachedService> cache;
  @Mock PentahoCacheTemplateConfiguration template;

  @Test
  public void testGetCache() throws Exception {
    when( cacheManager.getCache( ServiceCacheFactory.CACHE_NAME,
      CachedService.CacheKey.class, CachedService.class ) ).thenReturn( cache );
    ServiceCache serviceCache = serviceCacheFactory.createPushDown();

    assertThat( serviceCacheFactory.getCache( serviceCache ), is( cache ) );
  }

  @Test
  public void testCreateCache() throws Exception {
    when( cacheManager.getTemplates() ).thenReturn( ImmutableMap.of( TEMPLATE_NAME, template ) );
    when( template.createCache(
      ServiceCacheFactory.CACHE_NAME, CachedService.CacheKey.class, CachedService.class ) )
      .thenReturn( cache );

    ServiceCache serviceCache = serviceCacheFactory.createPushDown();

    assertThat( serviceCacheFactory.getTemplates(), contains( TEMPLATE_NAME ) );
    serviceCache.setTemplateName( TEMPLATE_NAME );
    assertThat( serviceCacheFactory.getCache( serviceCache ), is( cache ) );
  }
}
