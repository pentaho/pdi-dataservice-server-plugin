/*
 * ******************************************************************************
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
 *
 * ******************************************************************************
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
 */

package org.pentaho.di.trans.dataservice;

import org.pentaho.caching.api.PentahoCacheManager;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.dataservice.optimization.AutoOptimizationService;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.pentaho.di.trans.dataservice.serialization.DataServiceMetaStoreUtil;
import org.pentaho.di.trans.dataservice.streaming.StreamServiceKey;
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
}
