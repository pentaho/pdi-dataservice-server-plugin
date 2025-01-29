/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
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
import org.pentaho.di.ui.spoon.Spoon;

import java.util.Collections;
import java.util.function.Supplier;

public class DataServiceMetaFactory implements IDataServiceMetaFactory {
  private Supplier<Spoon> spoonSupplier;

  private PushDownFactory cacheFactory;

  public DataServiceMetaFactory() {
    this.spoonSupplier = Spoon::getInstance;
  }

  public DataServiceMetaFactory( Supplier<Spoon> spoonSupplier ) {
    this.spoonSupplier = spoonSupplier;
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

    dataServiceMeta.setName( createDataServiceName( step, rowLimit, false ) );
    dataServiceMeta.setStepname( step.getName() );
    dataServiceMeta.setRowLimit( rowLimit != null ?  rowLimit : 0 );

    PushDownOptimizationMeta pushDownMeta = new PushDownOptimizationMeta();
    pushDownMeta.setStepName( step.getName() );
    pushDownMeta.setType( getCacheFactory().createPushDown() );
    dataServiceMeta.setPushDownOptimizationMeta( Collections.singletonList( pushDownMeta ) );

    return dataServiceMeta;
  }

  private String createDataServiceName( StepMeta step, Integer rowLimit, boolean streaming ) throws KettleException {
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

    String local = isLocal( transMeta ) ? TransientResolver.LOCAL : "";
    String stream = streaming ? TransientResolver.STREAMING : "";
    return TransientResolver
      .buildTransient( fullFileName, local + stream + step.getName(),
        rowLimit );
  }

  private boolean isLocal( TransMeta transMeta ) {
    return spoonSupplier.get() != null && spoonSupplier.get().getActiveTransformation().equals( transMeta );
  }
}
