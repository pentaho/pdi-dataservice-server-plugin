package com.pentaho.di.trans.dataservice.optimization.cache;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import com.pentaho.di.trans.dataservice.optimization.PushDownOptTypeForm;
import com.pentaho.di.trans.dataservice.optimization.PushDownType;
import org.pentaho.caching.api.PentahoCacheManager;
import org.pentaho.caching.api.PentahoCacheTemplateConfiguration;

import javax.cache.Cache;
import javax.cache.CacheException;
import java.util.Map;

/**
 * @author nhudak
 */
public class ServiceCacheFactory implements PushDownFactory {
  public static final String CACHE_NAME = ServiceCache.class.getName();
  private final PentahoCacheManager cacheManager;

  public ServiceCacheFactory( PentahoCacheManager cacheManager ) {
    this.cacheManager = cacheManager;
  }

  @Override public String getName() {
    return ServiceCache.NAME;
  }

  @Override public Class<? extends PushDownType> getType() {
    return ServiceCache.class;
  }

  @Override public ServiceCache createPushDown() {
    return new ServiceCache( this );
  }

  @Override public PushDownOptTypeForm createPushDownOptTypeForm() {
    return new ServiceCacheOptForm( this );
  }

  public Cache<CachedServiceLoader.CacheKey, CachedServiceLoader> getCache( ServiceCache serviceCache )
    throws CacheException {
    String templateName = serviceCache.getTemplateName();
    Map<String, PentahoCacheTemplateConfiguration> templates = cacheManager.getTemplates();

    Cache<CachedServiceLoader.CacheKey, CachedServiceLoader> cache = cacheManager.getCache(
      CACHE_NAME,
      CachedServiceLoader.CacheKey.class,
      CachedServiceLoader.class
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
        CachedServiceLoader.CacheKey.class,
        CachedServiceLoader.class
      );
    }
  }

  public Iterable<String> getTemplates() {
    // TODO need better template filtering & selection
    return cacheManager.getTemplates().keySet();
  }
}
