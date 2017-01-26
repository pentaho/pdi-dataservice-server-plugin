/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
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

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.repository.RepositoryDirectory;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.resolvers.TransientResolver;
import org.pentaho.di.trans.step.StepMeta;

import java.util.Collections;

public class DataServiceMetaFactory implements IDataServiceMetaFactory {
  private static final Class<?> PKG = DataServiceMetaFactory.class;

  private PushDownFactory cacheFactory;

  public DataServiceMetaFactory() {
  }

  @Override public PushDownFactory getCacheFactory() {
    return this.cacheFactory;
  }

  @Override public void setCacheFactory( PushDownFactory cacheFactory ) {
    this.cacheFactory = cacheFactory;
  }

  @Override public DataServiceMeta createDataService( StepMeta step ) throws KettleException {
    return createDataService( step, null );
  }

  @Override public DataServiceMeta createDataService( StepMeta step, Integer rowLimit ) throws KettleException {
    TransMeta transformation = step.getParentTransMeta();

    DataServiceMeta dataServiceMeta = new DataServiceMeta( transformation );

    dataServiceMeta.setName( createDataServiceName( step, rowLimit ) );
    dataServiceMeta.setStepname( step.getName() );
    dataServiceMeta.setRowLimit( rowLimit );

    PushDownOptimizationMeta pushDownMeta = new PushDownOptimizationMeta();
    pushDownMeta.setStepName( step.getName() );
    pushDownMeta.setType( getCacheFactory().createPushDown() );
    dataServiceMeta.setPushDownOptimizationMeta( Collections.singletonList( pushDownMeta ) );

    return dataServiceMeta;
  }

  private String createDataServiceName( StepMeta step, Integer rowLimit ) throws KettleException {
    TransMeta transMeta = step.getParentTransMeta();
    String fullFileName;
    if ( !Utils.isEmpty( transMeta.getFilename() ) && transMeta.getObjectId() == null ) {
      fullFileName = transMeta.getFilename();
    } else {
      if ( transMeta.getRepositoryDirectory() != null ) {
        String path = transMeta.getRepositoryDirectory().getPath();
        if ( path.endsWith( RepositoryDirectory.DIRECTORY_SEPARATOR ) ) {
          fullFileName = path + transMeta.getName();
        } else {
          fullFileName = path + RepositoryDirectory.DIRECTORY_SEPARATOR + transMeta.getName();
        }
      } else {
        fullFileName = transMeta.getName();
      }
    }

    return TransientResolver.buildTransient( fullFileName, step.getName(), rowLimit );
  }
}
