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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.caching.api.PentahoCacheManager;
import org.pentaho.caching.api.PentahoCacheTemplateConfiguration;
import org.pentaho.di.trans.dataservice.optimization.cache.ui.ServiceCacheController;
import org.pentaho.di.trans.dataservice.optimization.cache.ui.ServiceCacheOverlay;

import javax.cache.Cache;
import java.util.concurrent.ExecutorService;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.pentaho.caching.api.Constants.CONFIG_TTL;

/**
 * @author nhudak
 */
@RunWith( MockitoJUnitRunner.StrictStubs.class)
public class ServiceCacheFactoryTest {

  public static final String TEMPLATE_NAME = "template";
  private static final String DATA_SERVICE_NAME = "dataServiceName";
  @Mock PentahoCacheManager cacheManager;
  @Mock ExecutorService executorService;
  @InjectMocks ServiceCacheFactory serviceCacheFactory;
  @Mock Cache<CachedService.CacheKey, CachedService> cache;
  @Mock PentahoCacheTemplateConfiguration template;

  @Test
  public void testFactory() throws Exception {
    assertThat( serviceCacheFactory.createPushDown(), isA( ServiceCache.class ) );
    assertThat( serviceCacheFactory.createOverlay(), isA( ServiceCacheOverlay.class ) );
    assertThat( serviceCacheFactory.createController(), isA( ServiceCacheController.class ) );
  }

  @Test
  public void testGetCache() throws Exception {
    assertThat( serviceCacheFactory.getCache( DATA_SERVICE_NAME ).isPresent(), is( false ) );
    when( cacheManager.getCache( cacheName(), CachedService.CacheKey.class, CachedService.class ) ).thenReturn( cache );
    ServiceCache serviceCache = serviceCacheFactory.createPushDown();

    assertThat( serviceCacheFactory.getCache( DATA_SERVICE_NAME ), is( Optional.of( cache ) ) );
    assertThat( serviceCacheFactory.getCache( serviceCache, DATA_SERVICE_NAME ), is( cache ) );
  }

  @Test
  public void testGetCacheFailure() throws Exception {
    when( cacheManager.getCache( anyString(), eq( CachedService.CacheKey.class ), eq( CachedService.class ) ) ).
      thenThrow( new RuntimeException() );

    assertThat( serviceCacheFactory.getCache( DATA_SERVICE_NAME ).isPresent(), is( false ) );
    verify( cacheManager ).destroyCache( cacheName() );
  }

  @Test
  public void testCreateCache() throws Exception {
    when( cacheManager.getTemplates() ).thenReturn( ImmutableMap.of( TEMPLATE_NAME, template ) );
    when( cacheManager.getCache( cacheName(), CachedService.CacheKey.class, CachedService.class ) ).thenReturn( cache );
    ServiceCache serviceCache = serviceCacheFactory.createPushDown();

    assertThat( serviceCacheFactory.getTemplateNames(), contains( TEMPLATE_NAME ) );
    serviceCache.setTemplateName( TEMPLATE_NAME );
    assertThat( serviceCacheFactory.getCache( serviceCache, DATA_SERVICE_NAME ), is( cache ) );
  }

  @Test
  public void testCreateCacheWithTtlOverride() throws Exception {
    when( cacheManager.getTemplates() ).thenReturn( ImmutableMap.of( TEMPLATE_NAME, template ) );
    PentahoCacheTemplateConfiguration overridenTemplate = mock( PentahoCacheTemplateConfiguration.class );

    when( overridenTemplate.createCache( cacheName(), CachedService.CacheKey.class, CachedService.class ) )
        .thenReturn( cache );
    when( template.overrideProperties( ImmutableMap.of( CONFIG_TTL, "1010" ) ) ).thenReturn( overridenTemplate );

    ServiceCache serviceCache = serviceCacheFactory.createPushDown();
    serviceCache.setTimeToLive( "1010" );

    assertThat( serviceCacheFactory.getTemplateNames(), contains( TEMPLATE_NAME ) );
    serviceCache.setTemplateName( TEMPLATE_NAME );
    assertThat( serviceCacheFactory.getCache( serviceCache, DATA_SERVICE_NAME ), is( cache ) );
  }

  private String cacheName() {
    return serviceCacheFactory.cacheName( DATA_SERVICE_NAME );
  }
}
