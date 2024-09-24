/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.pentaho.caching.api.PentahoCacheManager;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.dataservice.optimization.AutoOptimizationService;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.pentaho.di.trans.dataservice.serialization.DataServiceMetaStoreUtil;
import org.pentaho.di.trans.dataservice.streaming.StreamServiceKey;
import org.pentaho.di.trans.dataservice.streaming.execution.StreamingGeneratedTransExecution;
import org.pentaho.di.trans.dataservice.streaming.execution.StreamingServiceTransExecutor;
import org.pentaho.di.trans.dataservice.ui.DataServiceDelegate;
import org.pentaho.di.trans.dataservice.ui.UIFactory;
import org.pentaho.di.trans.dataservice.utils.DataServiceConstants;

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class DataServiceContext implements Context {
  private final DataServiceMetaStoreUtil metaStoreUtil;
  private final List<AutoOptimizationService> autoOptimizationServices;
  private final PentahoCacheManager cacheManager;
  private final List<PushDownFactory> pushDownFactories;
  private final LogChannelInterface logChannel;
  private final UIFactory uiFactory;

  //Cache for the generated tranformation executions, so that we can keep the same genTransExecution for multiple consumers
  private final Cache<String, StreamingGeneratedTransExecution> streamingGeneratedTransExecutionCache = CacheBuilder.newBuilder()
    .expireAfterAccess( 1, TimeUnit.DAYS )
    .removalListener( new RemovalListener<String, StreamingGeneratedTransExecution>() {
      public void onRemoval( RemovalNotification<String, StreamingGeneratedTransExecution> removal ) {
        removal.getValue().clearRowConsumers();
        logChannel.logDebug( DataServiceConstants.STREAMING_GEN_TRANS_CACHE_REMOVED + removal.getKey() );
      }
    } )
    .softValues()
    .build();

  // Use an in-memory cache with timed expiration and soft value references to prevent heap memory leaks
  private final ConcurrentMap<String, DataServiceExecutor> executors = CacheBuilder.newBuilder()
    .expireAfterAccess( 30, TimeUnit.SECONDS )
    .softValues()
    .<String, DataServiceExecutor>build().asMap();

  // Use an in-memory cache with soft value references to prevent heap memory leaks
  private final Cache<StreamServiceKey, StreamingServiceTransExecutor> serviceExecutors = CacheBuilder.newBuilder()
    .expireAfterAccess( 1, TimeUnit.DAYS )
    .removalListener( new RemovalListener<StreamServiceKey, StreamingServiceTransExecutor>() {
      public void onRemoval( RemovalNotification<StreamServiceKey, StreamingServiceTransExecutor> removal ) {
        StreamingServiceTransExecutor item = removal.getValue();
        LogChannelInterface log = item.getServiceTrans().getLogChannel();

        item.stopAll();

        log.logDebug( DataServiceConstants.STREAMING_SERVICE_CACHE_REMOVED + removal.getKey() );
      }
    } )
    .softValues()
    .<StreamServiceKey, StreamingServiceTransExecutor>build();

  public DataServiceContext( List<PushDownFactory> pushDownFactories,
                             List<AutoOptimizationService> autoOptimizationServices,
                             PentahoCacheManager cacheManager, UIFactory uiFactory, LogChannelInterface logChannel ) {
    this.pushDownFactories = pushDownFactories;
    this.autoOptimizationServices = autoOptimizationServices;
    this.cacheManager = cacheManager;
    this.metaStoreUtil = DataServiceMetaStoreUtil.create( this );
    this.logChannel = logChannel;
    this.uiFactory = uiFactory;
  }

  @VisibleForTesting
  protected DataServiceContext( List<PushDownFactory> pushDownFactories,
                                List<AutoOptimizationService> autoOptimizationServices,
                                PentahoCacheManager cacheManager, DataServiceMetaStoreUtil metaStoreUtil,
                                UIFactory uiFactory, LogChannelInterface logChannel ) {
    this.pushDownFactories = pushDownFactories;
    this.autoOptimizationServices = autoOptimizationServices;
    this.cacheManager = cacheManager;
    this.metaStoreUtil = metaStoreUtil;
    this.logChannel = logChannel;
    this.uiFactory = uiFactory;
  }

  @Override
  public PentahoCacheManager getCacheManager() {
    return cacheManager;
  }

  @Override
  public DataServiceMetaStoreUtil getMetaStoreUtil() {
    return metaStoreUtil;
  }

  @Override
  public List<AutoOptimizationService> getAutoOptimizationServices() {
    return autoOptimizationServices;
  }

  @Override
  public List<PushDownFactory> getPushDownFactories() {
    return pushDownFactories;
  }

  @Override
  public LogChannelInterface getLogChannel() {
    return logChannel;
  }

  @Override
  public UIFactory getUIFactory() {
    return this.uiFactory;
  }

  @Override
  public DataServiceDelegate getDataServiceDelegate() {
    return DataServiceDelegate.withDefaultSpoonInstance( this );
  }

  @Override
  public void addExecutor( DataServiceExecutor executor ) {
    executors.putIfAbsent( executor.getId(), executor );
  }

  @Override
  public DataServiceExecutor getExecutor( String id ) {
    return executors.get( id );
  }

  @Override
  public void removeExecutor( String id ) {
    executors.remove( id );
  }

  @Override
  public void addServiceTransExecutor( final StreamingServiceTransExecutor serviceExecutor ) {
    serviceExecutors.put( serviceExecutor.getKey(), serviceExecutor );
  }

  @Override
  public StreamingServiceTransExecutor getServiceTransExecutor( StreamServiceKey key ) {
    return serviceExecutors.getIfPresent( key );
  }

  @Override
  public void removeServiceTransExecutor( StreamServiceKey key ) {
    serviceExecutors.invalidate( key );
    serviceExecutors.cleanUp();
  }

  @Override
  public void removeServiceTransExecutor( String dataServiceName ) {
    for ( StreamServiceKey key : serviceExecutors.asMap().keySet() ) {
      if ( key.getDataServiceId().equals( dataServiceName ) ) {
        removeServiceTransExecutor( key );
      }
    }
  }

  @Override
  public StreamingGeneratedTransExecution getStreamingGeneratedTransExecution( String key ) {
    if ( key != null ) {
      return this.streamingGeneratedTransExecutionCache.getIfPresent( key );
    }
    return null;
  }

  @Override
  public void addStreamingGeneratedTransExecution( String key,
                                                   StreamingGeneratedTransExecution streamingGeneratedTransExecution ) {
    if ( key != null ) {
      this.streamingGeneratedTransExecutionCache.put( key, streamingGeneratedTransExecution );
    }
  }

  @Override
  public void removeStreamingGeneratedTransExecution( String key ) {
    if ( key != null ) {
      this.streamingGeneratedTransExecutionCache.invalidate( key );
      this.streamingGeneratedTransExecutionCache.cleanUp();
    }
  }
}
