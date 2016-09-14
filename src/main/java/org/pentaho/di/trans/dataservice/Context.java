/*
 * ******************************************************************************
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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
import org.pentaho.di.trans.dataservice.ui.DataServiceDelegate;
import org.pentaho.di.trans.dataservice.ui.UIFactory;

import java.util.List;

/**
 * Created by rfellows on 7/14/16.
 */
public interface Context {
  PentahoCacheManager getCacheManager();

  DataServiceMetaStoreUtil getMetaStoreUtil();

  List<AutoOptimizationService> getAutoOptimizationServices();

  List<PushDownFactory> getPushDownFactories();

  LogChannelInterface getLogChannel();

  UIFactory getUIFactory();

  DataServiceDelegate getDataServiceDelegate();

  void addExecutor( DataServiceExecutor executor );

  DataServiceExecutor getExecutor( String id );

  void removeExecutor( String id );
}
