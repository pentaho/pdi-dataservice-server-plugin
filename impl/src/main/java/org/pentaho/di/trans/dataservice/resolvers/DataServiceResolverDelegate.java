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

import com.google.common.base.Function;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

public class DataServiceResolverDelegate implements DataServiceResolver {

  private List<DataServiceResolver> resolvers;

  public DataServiceResolverDelegate() {
    resolvers = new ArrayList<>();
  }

  public DataServiceResolverDelegate( List<DataServiceResolver> resolvers ) {
    this.resolvers = resolvers;
  }

  public void addResolver( DataServiceResolver resolver ) {
    resolvers.add( resolver );
  }

  public void removeResolver( DataServiceResolver resolver ) {
    resolvers.remove( resolver );
  }

  @Override public List<DataServiceMeta> getDataServices( Function<Exception, Void> logger ) {
    List<DataServiceMeta> dataServiceMetas = new ArrayList<>();
    for ( DataServiceResolver resolver : resolvers ) {
      dataServiceMetas.addAll( resolver.getDataServices( logger ) );
    }
    return dataServiceMetas;
  }

  @Override public List<DataServiceMeta> getDataServices( String dataServiceName, Function<Exception, Void> logger ) {
    List<DataServiceMeta> dataServiceMetas = new ArrayList<>();
    for ( DataServiceResolver resolver : resolvers ) {
      dataServiceMetas.addAll( resolver.getDataServices( dataServiceName, logger ) );
    }
    return dataServiceMetas;
  }

  @Override public DataServiceMeta getDataService( String dataServiceName ) {
    for ( DataServiceResolver resolver : resolvers ) {
      DataServiceMeta dataServiceMeta = resolver.getDataService( dataServiceName );
      if ( dataServiceMeta != null ) {
        return dataServiceMeta;
      }
    }
    return null;
  }

  @Override public List<String> getDataServiceNames( String dataServiceName ) {
    List<String> dataServiceNames = new ArrayList<>();

    for ( DataServiceResolver resolver : resolvers ) {
      dataServiceNames.addAll( resolver.getDataServiceNames( dataServiceName ) );
    }

    return dataServiceNames;
  }

  @Override public DataServiceExecutor.Builder createBuilder( SQL sql ) throws KettleException {
    boolean foundDataService = false;
    for ( DataServiceResolver resolver : resolvers ) {
      DataServiceExecutor.Builder builder = resolver.createBuilder( sql );
      if ( builder != null ) {
        return builder;
      } else {
        foundDataService = foundDataService || ( resolver.getDataService( sql.getServiceName() ) != null );
      }
    }
    throw new KettleException( foundDataService ? "Error when creating builder for sql query"
      : MessageFormat.format( "Data Service {0} was not found", sql.getServiceName() ) );
  }

  @Override public List<String> getDataServiceNames() {
    List<String> dataServiceNames = new ArrayList<>();
    for ( DataServiceResolver resolver : resolvers ) {
      dataServiceNames.addAll( resolver.getDataServiceNames() );
    }
    return dataServiceNames;
  }
}
