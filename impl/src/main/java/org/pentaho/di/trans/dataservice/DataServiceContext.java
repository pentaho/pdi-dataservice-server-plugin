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
import com.google.common.cache.CacheBuilder;
import org.pentaho.caching.api.PentahoCacheManager;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.dataservice.optimization.AutoOptimizationService;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.pentaho.di.trans.dataservice.serialization.DataServiceMetaStoreUtil;
import org.pentaho.di.trans.dataservice.streaming.execution.StreamingServiceTransExecutor;
import org.pentaho.di.trans.dataservice.ui.DataServiceDelegate;
import org.pentaho.di.trans.dataservice.ui.UIFactory;

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

  // Use an in-memory cache with timed expiration and soft value references to prevent heap memory leaks
  private final ConcurrentMap<String, DataServiceExecutor> executors = CacheBuilder.newBuilder()
    .expireAfterAccess( 30, TimeUnit.SECONDS )
    .softValues()
    .<String, DataServiceExecutor>build().asMap();

  // Use an in-memory cache with timed expiration and soft value references to prevent heap memory leaks
  private final ConcurrentMap<String, StreamingServiceTransExecutor> serviceExecutors = CacheBuilder.newBuilder()
    .expireAfterAccess( 30, TimeUnit.SECONDS )
    .softValues()
    .<String, StreamingServiceTransExecutor>build().asMap();

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
    serviceExecutors.putIfAbsent( serviceExecutor.getId(), serviceExecutor );
  }

  @Override
  public final StreamingServiceTransExecutor getServiceTransExecutor( String id ) {
    return serviceExecutors.get( id );
  }

  @Override
  public void removeServiceTransExecutor( String id ) {
    serviceExecutors.remove( id );
  }
}
