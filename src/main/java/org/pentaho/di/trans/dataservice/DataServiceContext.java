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

package org.pentaho.di.trans.dataservice;

import org.pentaho.di.trans.dataservice.cache.DataServiceMetaCache;
import org.pentaho.di.trans.dataservice.optimization.AutoOptimizationService;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.pentaho.di.trans.dataservice.serialization.DataServiceMetaStoreUtil;

import java.util.List;

public class DataServiceContext {
  private final DataServiceMetaStoreUtil metaStoreUtil;
  private final List<AutoOptimizationService> autoOptimizationServices;
  private final List<PushDownFactory> pushDownFactories;

  public DataServiceContext( List<PushDownFactory> pushDownFactories,
                             List<AutoOptimizationService> autoOptimizationServices,
                             DataServiceMetaCache cache ) {
    this.pushDownFactories = pushDownFactories;
    this.autoOptimizationServices = autoOptimizationServices;
    this.metaStoreUtil = new DataServiceMetaStoreUtil( pushDownFactories, cache );
  }

  public DataServiceMetaStoreUtil getMetaStoreUtil() {
    return metaStoreUtil;
  }

  public List<AutoOptimizationService> getAutoOptimizationServices() {
    return autoOptimizationServices;
  }

  public List<PushDownFactory> getPushDownFactories() {
    return pushDownFactories;
  }
}
