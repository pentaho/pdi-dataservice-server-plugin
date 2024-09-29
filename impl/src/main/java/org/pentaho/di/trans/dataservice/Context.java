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


package org.pentaho.di.trans.dataservice;

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

import java.util.List;

/**
 * Data Services context interface.
 */
public interface Context {
  /**
   * Getter for the context {@link PentahoCacheManager} cache manager.
   *
   * @return the context {@link PentahoCacheManager} cache manager.
   */
  PentahoCacheManager getCacheManager();

  /**
   * Getter for the context {@link DataServiceMetaStoreUtil} meta store util.
   *
   * @return the context {@link DataServiceMetaStoreUtil} meta store util.
   */
  DataServiceMetaStoreUtil getMetaStoreUtil();

  /**
   * Getter for the context {@link AutoOptimizationService} auto optimizations services list.
   *
   * @return the context {@link AutoOptimizationService} auto optimizations services list.
   */
  List<AutoOptimizationService> getAutoOptimizationServices();

  /**
   * Getter for the context {@link PushDownFactory} push down factories list.
   *
   * @return the context {@link PushDownFactory} push down factories list.
   */
  List<PushDownFactory> getPushDownFactories();

  /**
   * Getter for the context {@link LogChannelInterface} log channel.
   *
   * @return the context {@link LogChannelInterface} log channel.
   */
  LogChannelInterface getLogChannel();

  /**
   * Getter for the context {@link UIFactory} UI factory.
   *
   * @return the context {@link UIFactory} UI factory.
   */
  UIFactory getUIFactory();

  /**
   * Getter for the context {@link DataServiceDelegate} data services delegate.
   *
   * @return the context {@link DataServiceDelegate} data services delegate.
   */
  DataServiceDelegate getDataServiceDelegate();

  /**
   * Add {@link DataServiceExecutor} to the executors cache.
   * @param executor the {@link DataServiceExecutor} to cache.
   */
  void addExecutor( DataServiceExecutor executor );

  /**
   * Get {@link DataServiceExecutor} from the executors cache.
   * @param id The ID of the requested executor.
   *
   * @return The {@link DataServiceExecutor} with the given ID from cache if exists, null otherwise.
   */
  DataServiceExecutor getExecutor( String id );

  /**
   * Removes the {@link DataServiceExecutor} with the given ID from the executors cache.
   *
   * @param id The ID of the executor to me removed.
   */
  void removeExecutor( String id );

  /**
   * Add {@link StreamingServiceTransExecutor} to the Service Transformations executions cache.
   *
   * @param serviceExecutor the {@link StreamingServiceTransExecutor} to cache.
   */
  void addServiceTransExecutor( StreamingServiceTransExecutor serviceExecutor );

  /**
   * Get {@link StreamingServiceTransExecutor} from the service transformations executions cache.
   *
   * @param key The key of the requested executor.
   * @return The {@link StreamingServiceTransExecutor} with the given key from cache if exists, null otherwise.
   */
  StreamingServiceTransExecutor getServiceTransExecutor( StreamServiceKey key );

  /**
   * Removes the {@link StreamingServiceTransExecutor} with the given key from the
   * service transformations executors cache.
   *
   * @param key The key of the executor to be removed.
   */
  void removeServiceTransExecutor( StreamServiceKey key );

  /**
   * Removes all {@link StreamingServiceTransExecutor} with the given data service name from the
   * service transformations executors cache.
   *
   * @param dataServiceName The name of the data service to be removed.
   */
  void removeServiceTransExecutor( String dataServiceName );

  /**
   * Gets the {@link StreamingGeneratedTransExecution} by using the key used to cache/store it.
   * @param key The key to use
   * @return The cached {@link StreamingGeneratedTransExecution}, or null if it doesn't exist
   */
  StreamingGeneratedTransExecution getStreamingGeneratedTransExecution( String key );

  /**
   * Adds a {@link StreamingGeneratedTransExecution} to the cache of streaming generated transformation executions.
   * @param key The key used to index the {@link StreamingGeneratedTransExecution} to cache.
   * @param streamingGeneratedTransExecution the {@link StreamingGeneratedTransExecution} instance to cache.
   */
  void addStreamingGeneratedTransExecution( String key, StreamingGeneratedTransExecution streamingGeneratedTransExecution );

  /**
   * Removes a {@link StreamingGeneratedTransExecution} from the cache/store by key.
   * @param key The key used to find the {@link StreamingGeneratedTransExecution} instance to remove from the cache/store.
   */
  void removeStreamingGeneratedTransExecution( String key );
}
