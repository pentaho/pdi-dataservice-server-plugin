/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.optimization.OptimizationImpactInfo;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.StepOptimization;
import org.pentaho.di.trans.step.StepInterface;

import java.util.HashMap;
import java.util.Map;

public class BackgroundCache extends StepOptimization {
  private ServiceCache serviceCache;

  private static Map<String, Trans> transMap = new HashMap<>();
  public BackgroundCache( ServiceCache serviceCache ) {
    this.serviceCache = serviceCache;
  }

  public ServiceCache getServiceCache() {
    return serviceCache;
  }

  @Override public void init( final TransMeta transMeta, final DataServiceMeta dataService,
                              final PushDownOptimizationMeta optMeta ) {
    serviceCache.init( transMeta, dataService, optMeta );
  }

  @Override protected boolean activate( final DataServiceExecutor executor, final StepInterface stepInterface ) {
    if ( transMap.containsKey( executor.getServiceName() ) ) {
      Trans trans = transMap.get( executor.getServiceName() );
      if ( trans.isRunning() ) {
        trans.waitUntilFinished();
      }
      transMap.remove( executor.getServiceName() );
    }

    boolean cacheExists = serviceCache.activate( executor, stepInterface );
    if ( !cacheExists ) {
      transMap.put( executor.getServiceName(), executor.getServiceTrans() );
    }
    return cacheExists;
  }

  @Override
  protected OptimizationImpactInfo preview( final DataServiceExecutor executor, final StepInterface stepInterface ) {
    return serviceCache.preview( executor, stepInterface );
  }
}
