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

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.pentaho.caching.api.PentahoCacheTemplateConfiguration;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.optimization.OptimizationImpactInfo;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownType;
import org.pentaho.di.trans.dataservice.optimization.StepOptimization;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.metastore.persist.MetaStoreAttribute;

import javax.cache.Cache;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.base.Predicates.not;
import static com.google.common.base.Predicates.notNull;
import static org.pentaho.caching.api.Constants.CONFIG_TTL;
import static org.pentaho.caching.api.Constants.DEFAULT_TEMPLATE;

/**
 * @author nhudak
 */
public class ServiceCache extends StepOptimization {
  public static final String NAME = "Service Cache";
  private final ServiceCacheFactory factory;
  public static final String SERVICE_CACHE_TEMPLATE_NAME = "template_name";
  public static final String SERVICE_CACHE_TTL = "time_to_live";

  @MetaStoreAttribute( key = SERVICE_CACHE_TEMPLATE_NAME )
  private String templateName = DEFAULT_TEMPLATE;

  @MetaStoreAttribute( key = SERVICE_CACHE_TTL )
  private String timeToLive;

  public ServiceCache( ServiceCacheFactory factory ) {
    this.factory = factory;
  }

  @Override public void init( TransMeta transMeta, DataServiceMeta dataService, PushDownOptimizationMeta optMeta ) {
    optMeta.setStepName( dataService.getStepname() );
  }

  @Override public boolean activate( final DataServiceExecutor executor, StepInterface stepInterface ) {
    final LogChannelInterface logChannel = executor.getGenTrans().getLogChannel();

    // Check for any cache entries that may answer this query
    for ( CachedService availableCache : getAvailableCache( executor ).values() ) {
      try {
        ListenableFuture<Integer> replay = factory.createCachedServiceLoader( availableCache ).replay( executor );
        Futures.addCallback( replay, new FutureCallback<Integer>() {
          @Override public void onSuccess( Integer rowCount ) {
            logChannel.logBasic( "Service Transformation successfully replayed " + rowCount + " rows from cache" );
          }

          @Override public void onFailure( Throwable t ) {
            logChannel.logError( "Cache failed to replay service transformation", t );
          }
        }, factory.getExecutorService() );
        return true;
      } catch ( Throwable e ) {
        logChannel.logError( "Unable to replay from cache", e );
      }
    }

    // Allow service transformation to run, observe rows
    Futures.addCallback( factory.createObserver( executor ).install(), new FutureCallback<CachedService>() {
      @Override public void onSuccess( CachedService result ) {
        if ( executor.isStopped() || executor.hasErrors() ) {
          return;
        }

        Cache<CachedService.CacheKey, CachedService>
            cache =
            factory.getCache( ServiceCache.this, executor.getServiceName() );
        CachedService.CacheKey key = createRootKey( executor );
        // If result set is complete, order is not important
        if ( result.isComplete() ) {
          key = key.withoutOrder();
        }
        if ( cache.putIfAbsent( key, result ) ) {
          logChannel.logBasic( "Service Transformation results cached", key );
        } else {
          try {
            CachedService existing = checkNotNull( cache.get( key ) );
            // If the existing result set can't answer this query, replace it
            if ( !existing.answersQuery( executor ) && cache.replace( key, existing, result ) ) {
              logChannel.logBasic( "Service Transformation cache updated", key );
            } else {
              logChannel.logDetailed( "Service Transformation cache was not updated", key );
            }
          } catch ( Throwable t ) {
            onFailure( t );
          }
        }
      }

      @Override public void onFailure( Throwable t ) {
        logChannel.logError( "Cache failed to observe service transformation", t );
      }
    }, factory.getExecutorService() );
    return false;
  }

  @Override public OptimizationImpactInfo preview( DataServiceExecutor executor, StepInterface stepInterface ) {
    OptimizationImpactInfo info = new OptimizationImpactInfo( executor.getService().getStepname() );
    Map<CachedService.CacheKey, CachedService> availableCache = getAvailableCache( executor );
    for ( Map.Entry<CachedService.CacheKey, CachedService> available : availableCache.entrySet() ) {
      info.setModified( true );
      info.setQueryBeforeOptimization( MessageFormat.format( "Service results for {0} are available.",
          available.getKey() ) );
      info.setQueryAfterOptimization( MessageFormat.format( "{0} rows can be read from cache.",
          available.getValue().getRowMetaAndData().size() ) );
      return info;
    }
    info.setModified( false );
    info.setQueryBeforeOptimization( "Service results are not available. Execute this query to cache results." );
    return info;
  }

  public CachedService.CacheKey createRootKey( DataServiceExecutor executor ) {
    CachedService.CacheKey rootKey = CachedService.CacheKey.create( executor );
    // If query is not optimized, the where clause can be ignored
    if ( !isPushDownOptimized( executor ) ) {
      rootKey = rootKey.withoutCondition();
    }
    return rootKey;
  }

  Map<CachedService.CacheKey, CachedService> getAvailableCache( final DataServiceExecutor executor ) {
    final Cache<CachedService.CacheKey, CachedService> cache =
        maybeInvalidateCache( executor );
    if ( cache == null ) {
      return ImmutableMap.of();
    }

    // First test if the rootKey entry answers the query
    CachedService.CacheKey rootKey = createRootKey( executor );
    CachedService exactMatch = cache.get( rootKey );
    if ( exactMatch != null && exactMatch.answersQuery( executor ) ) {
      return ImmutableMap.of( rootKey, exactMatch );
    }

    // Otherwise, check all related keys for a complete set
    return FluentIterable.from( rootKey.all() )
      .transform( new Function<CachedService.CacheKey, Map<CachedService.CacheKey, CachedService>>() {
        @Override public Map<CachedService.CacheKey, CachedService> apply(
          CachedService.CacheKey key ) {
          CachedService value = cache.get( key );
          return value != null && value.isComplete() ? ImmutableMap.of( key, value ) : null;
        }
      } )
      .filter( notNull() )
      .first().or( ImmutableMap.<CachedService.CacheKey, CachedService>of() );
  }

  /**
   * Checks whether cache configuration has changed in such a way that the existing cache is
   * no longer valid.  Will return the cache associated with the data service (if available) otherwise.
   */
  private Cache<CachedService.CacheKey, CachedService> maybeInvalidateCache(
      DataServiceExecutor executor ) {
    Optional<Cache<CachedService.CacheKey, CachedService>> cache = factory.getCache( executor.getServiceName() );
    LogChannelInterface logChannel = executor.getServiceTrans().getLogChannel();
    if ( cache.isPresent() ) {
      if ( !ttlMatches( cache.get(), logChannel ) ) {
        logChannel.logBasic( "Dropping cache associated with " + executor.getServiceName() );
        dropCache( cache.get() );
      } else {
        logChannel.logDebug( "Found cache associated with " + executor.getServiceName() );
        return cache.get();
      }
    }
    return null;
  }

  private boolean ttlMatches( Cache<CachedService.CacheKey, CachedService> cache, LogChannelInterface log ) {
    CompleteConfiguration config = cache.getConfiguration( CompleteConfiguration.class );
    if ( getTimeToLive() == null ) {
      // ttl has not been modified
      return true;
    }
    if ( config != null ) {
      try {
        Duration duration = getConfigDuration( config );
        long ttl = Long.parseLong( getTimeToLive() );
        return ttl == duration.getDurationAmount();
      } catch ( NumberFormatException nfe ) {
        log.logError(
            String.format( "Failed to determine configured TTL value for cache '%s'.  TTL value = '%s'",
                cache.getName(), getTimeToLive() ) );
        throw nfe;
      }
    }
    log.logError(
      String.format(
          "Failed to check TTL consistency with cache for name '%s'.\n  Assuming cache can be used", cache.getName()
      )
    );
    return true;
  }

  private synchronized void dropCache( Cache<CachedService.CacheKey, CachedService> cache ) {
    if ( !cache.isClosed() ) {
      cache.clear();
      cache.close();
    }
  }

  private Duration getConfigDuration( CompleteConfiguration config ) {
    ExpiryPolicy policy = (ExpiryPolicy) config.getExpiryPolicyFactory().create();
    return policy.getExpiryForAccess();
  }

  /**
   * Checks if a query has push down optimizations other than ServiceCache.
   *
   * @param executor query to check
   * @return <code>true</code> if other optimizations are found,
   * <code>false</code> if no optimizations are defined or all are of <code>ServiceClass</code> type
   */
  boolean isPushDownOptimized( DataServiceExecutor executor ) {
    return FluentIterable.from( executor.getService().getPushDownOptimizationMeta() )
      .transform( new Function<PushDownOptimizationMeta, PushDownType>() {
        @Override public PushDownType apply( PushDownOptimizationMeta input ) {
          return input.getType();
        }
      } )
      .anyMatch( not( instanceOf( ServiceCache.class ) ) );
  }

  public String getTemplateName() {
    return templateName;
  }

  public void setTemplateName( String templateName ) {
    this.templateName = templateName;
  }

  public String getTimeToLive() {
    return timeToLive;
  }

  public void setTimeToLive( String timeToLive ) {
    this.timeToLive = timeToLive;
  }

  public String getConfiguredTimeToLive() {
    PentahoCacheTemplateConfiguration configuration = factory.getPentahoCacheTemplateConfiguration( this );
    return configuration.getProperties().get( CONFIG_TTL );
  }

  public Map<String, String> getTemplateOverrides() {
    if ( getTimeToLive() != null ) {
      return ImmutableMap.of( CONFIG_TTL, getTimeToLive() );
    } else {
      return Collections.emptyMap();
    }
  }

}
