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

package org.pentaho.di.trans.dataservice.clients;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.reactivex.Observer;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.service.PluginServiceLoader;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;
import org.pentaho.di.trans.dataservice.resolvers.DataServiceResolver;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.locator.api.MetastoreLocator;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author nhudak
 */
public class ExecutorQueryService implements Query.Service {

  private DataServiceResolver resolver;
  private MetastoreLocator metastoreLocator;

  @VisibleForTesting
  ExecutorQueryService( DataServiceResolver resolver, MetastoreLocator metastoreLocator ) {
    this.resolver = resolver;
    this.metastoreLocator = metastoreLocator;
  }

  // OSGi blueprint constructor
  public ExecutorQueryService( DataServiceResolver resolver ) {
    this.resolver = resolver;
  }

  private synchronized MetastoreLocator getMetaStoreLocator() {
    if ( metastoreLocator == null ) {
      try {
        Collection<MetastoreLocator> metastoreLocators = PluginServiceLoader.loadServices( MetastoreLocator.class );
        metastoreLocator = metastoreLocators.stream().findFirst().get();
      } catch ( Exception e ) {
        LogChannel.GENERAL.logError( "Error getting MetastoreLocator", e );
        throw new IllegalStateException( e );
      }
    }
    return this.metastoreLocator;
  }

  @Override public Query prepareQuery( String sqlString, int maxRows, Map<String, String> parameters )
    throws KettleException {
    SQL sql = new SQL( sqlString );
    Query query;
    try {
      IMetaStore metaStore = getMetaStoreLocator() != null ? getMetaStoreLocator().getMetastore() : null;
      DataServiceExecutor executor = resolver.createBuilder( sql )
        .rowLimit( maxRows )
        .parameters( parameters )
        .metastore( metaStore )
        .build();
      query = new ExecutorQuery( executor );
    } catch ( Exception e ) {
      Throwables.propagateIfInstanceOf( e, KettleException.class );
      throw new KettleException( e );
    }
    return query;
  }

  @Override public Query prepareQuery( String sqlString, IDataServiceClientService.StreamingMode windowMode,
                                       long windowSize, long windowEvery, long windowLimit,
                                       final Map<String, String> parameters ) throws KettleException {
    SQL sql = new SQL( sqlString );
    Query query;
    try {
      IMetaStore metaStore = getMetaStoreLocator() != null ? getMetaStoreLocator().getMetastore() : null;
      DataServiceExecutor executor = resolver.createBuilder( sql )
        .rowLimit( 0 )
        .windowMode( windowMode )
        .windowSize( windowSize )
        .windowEvery( windowEvery )
        .windowLimit( windowLimit )
        .parameters( parameters )
        .metastore( metaStore )
        .build();
      query = new ExecutorQuery( executor );
    } catch ( Exception e ) {
      Throwables.propagateIfInstanceOf( e, KettleException.class );
      throw new KettleException( e );
    }
    return query;
  }

  public static DataOutputStream asDataOutputStream( OutputStream outputStream ) {
    return outputStream instanceof DataOutputStream
        ? ( (DataOutputStream) outputStream )
        : new DataOutputStream( outputStream );
  }

  private static class ExecutorQuery implements Query {

    private final DataServiceExecutor executor;

    public ExecutorQuery( DataServiceExecutor executor ) {
      this.executor = executor;
    }

    @Override
    public void writeTo( OutputStream outputStream ) throws IOException {
      DataServiceExecutor dataServiceExecutor = executor.executeQuery( asDataOutputStream( outputStream ) );
      if ( dataServiceExecutor != null ) {
        dataServiceExecutor.waitUntilFinished();
      }
    }

    @Override public List<Trans> getTransList() {
      return ImmutableList.of( executor.getServiceTrans(), executor.getGenTrans() );
    }

    @Override
    public void pushTo( Observer<List<RowMetaAndData>> streamingWindowConsumer ) throws Exception {
      executor.executeStreamingQuery( streamingWindowConsumer, false );
    }
  }
}
