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

package org.pentaho.di.trans.dataservice.resolvers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.service.PluginServiceLoader;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.ui.DataServiceDelegate;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.kettle.repository.locator.api.KettleRepositoryLocator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by bmorrise on 8/30/16.
 */
public class MetaStoreResolver implements DataServiceResolver {

  private DataServiceDelegate delegate;

  // OSGi constructor
  public MetaStoreResolver( DataServiceContext context ) {
    KettleRepositoryLocator repositoryLocator;
    try {
      Collection<KettleRepositoryLocator> metastoreLocators = PluginServiceLoader.loadServices( KettleRepositoryLocator.class );
      repositoryLocator = metastoreLocators.stream().findFirst().get();
    } catch ( Exception e ) {
      LogChannel.GENERAL.logError( "Error getting MetastoreLocator", e );
      throw new IllegalStateException( e );
    }
    initialize( repositoryLocator, context );
  }

  @VisibleForTesting
  public MetaStoreResolver( KettleRepositoryLocator repositoryLocator, DataServiceContext context ) {
    initialize( repositoryLocator, context );
  }

  private void initialize( KettleRepositoryLocator repositoryLocator, DataServiceContext context ) {
    if ( repositoryLocator != null ) {
      this.delegate = new DataServiceDelegate( context ) {
        @Override public Repository getRepository() {
          return repositoryLocator.getRepository();
        }
      };
    } else {
      this.delegate = DataServiceDelegate.withDefaultSpoonInstance( context );
    }
  }

  @Override
  public List<DataServiceMeta> getDataServices( String dataServiceName, Function<Exception, Void> logger ) {
    return getDataServices( logger );
  }

  @Override
  public List<DataServiceMeta> getDataServices( Function<Exception, Void> logger ) {
    return (List<DataServiceMeta>) delegate.getDataServices( logger );
  }

  @Override
  public DataServiceMeta getDataService( String dataServiceName ) {
    try {
      return delegate.getDataService( dataServiceName );
    } catch ( MetaStoreException e ) {
      return null;
    }
  }

  @Override public List<String> getDataServiceNames( String dataServiceName ) {
    return getDataServiceNames();
  }

  @Override public DataServiceExecutor.Builder createBuilder( SQL sql ) {
    try {
      return delegate.createBuilder( sql );
    } catch ( MetaStoreException e ) {
      return null;
    }
  }

  @Override public List<String> getDataServiceNames() {
    try {
      return delegate.getDataServiceNames();
    } catch ( MetaStoreException e ) {
      return new ArrayList<>();
    }
  }
}
