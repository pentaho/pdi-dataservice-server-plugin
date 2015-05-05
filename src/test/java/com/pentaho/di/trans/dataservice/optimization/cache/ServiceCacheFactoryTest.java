package com.pentaho.di.trans.dataservice.optimization.cache;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.caching.api.PentahoCacheManager;
import org.pentaho.caching.api.PentahoCacheTemplateConfiguration;

import javax.cache.Cache;

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
  @InjectMocks ServiceCacheFactory serviceCacheFactory;
  @Mock Cache<CachedServiceLoader.CacheKey, CachedServiceLoader> cache;
  @Mock PentahoCacheTemplateConfiguration template;

  @Test
  public void testGetCache() throws Exception {
    when( cacheManager.getCache( ServiceCacheFactory.CACHE_NAME,
      CachedServiceLoader.CacheKey.class, CachedServiceLoader.class ) ).thenReturn( cache );
    ServiceCache serviceCache = serviceCacheFactory.createPushDown();

    assertThat( serviceCacheFactory.getCache( serviceCache ), is( cache ) );
  }

  @Test
  public void testCreateCache() throws Exception {
    when( cacheManager.getTemplates() ).thenReturn( ImmutableMap.of( TEMPLATE_NAME, template ) );
    when( template.createCache(
      ServiceCacheFactory.CACHE_NAME, CachedServiceLoader.CacheKey.class, CachedServiceLoader.class ) )
      .thenReturn( cache );

    ServiceCache serviceCache = serviceCacheFactory.createPushDown();

    assertThat( serviceCacheFactory.getTemplates(), contains( TEMPLATE_NAME ) );
    serviceCache.setTemplateName( TEMPLATE_NAME );
    assertThat( serviceCacheFactory.getCache( serviceCache ), is( cache ) );
  }
}
