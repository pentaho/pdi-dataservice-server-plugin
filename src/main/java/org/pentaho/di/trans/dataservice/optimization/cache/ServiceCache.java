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

package org.pentaho.di.trans.dataservice.optimization.cache;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.optimization.OptimizationImpactInfo;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownType;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.metastore.persist.MetaStoreAttribute;

import javax.cache.Cache;
import java.text.MessageFormat;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.base.Predicates.not;
import static com.google.common.base.Predicates.notNull;
import static org.pentaho.caching.api.Constants.DEFAULT_TEMPLATE;

/**
 * @author nhudak
 */
public class ServiceCache implements PushDownType {
  public static final String NAME = "Service Cache";
  private final ServiceCacheFactory factory;
  public static final String SERVICE_CACHE_TEMPLATE_NAME = "template_name";

  @MetaStoreAttribute( key = SERVICE_CACHE_TEMPLATE_NAME ) private String templateName = DEFAULT_TEMPLATE;

  public ServiceCache( ServiceCacheFactory factory ) {
    this.factory = factory;
  }

  @Override public String getTypeName() {
    return NAME;
  }

  @Override public void init( TransMeta transMeta, DataServiceMeta dataService, PushDownOptimizationMeta optMeta ) {
    optMeta.setStepName( dataService.getStepname() );
  }

  @Override public boolean activate( final DataServiceExecutor executor, StepInterface stepInterface ) {
    final LogChannelInterface logChannel = executor.getGenTrans().getLogChannel();
    final Cache<CachedService.CacheKey, CachedService> cache = factory.getCache( this );

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
    final Cache<CachedService.CacheKey, CachedService> cache = factory.getCache( this );
    CachedService.CacheKey rootKey = createRootKey( executor );

    // First test if the rootKey entry answers the query
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

}
