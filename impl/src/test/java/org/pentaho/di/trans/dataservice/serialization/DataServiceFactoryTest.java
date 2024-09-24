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

package org.pentaho.di.trans.dataservice.serialization;

import com.google.common.base.Supplier;
import org.junit.Test;
import org.mockito.Mock;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.clients.DataServiceClient;
import org.pentaho.di.trans.dataservice.clients.Query;
import org.pentaho.di.trans.dataservice.resolvers.DataServiceResolver;
import org.pentaho.di.trans.dataservice.resolvers.MetaStoreResolver;
import org.pentaho.kettle.repository.locator.api.KettleRepositoryLocator;
import org.pentaho.metastore.api.IMetaStore;

import java.util.concurrent.Executors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */
public class DataServiceFactoryTest extends DataServiceMetaStoreUtilTest {
  DataServiceFactory factory;

  @Mock Supplier<Repository> supplier;
  @Mock Supplier<IMetaStore> metaStoreSupplier;
  @Mock Query.Service queryService;
  @Mock KettleRepositoryLocator repositoryLocator;
  @Mock DataServiceContext context;
  @Mock SQL sql;
  @Mock DataServiceMeta dsMeta;

  public void setUp() throws Exception {
    super.setUp();

    when( sql.getServiceName() ).thenReturn( DATA_SERVICE_NAME );
    when( supplier.get() ).thenReturn( repository );
    when( context.getMetaStoreUtil() ).thenReturn( metaStoreUtil );
    factory = new DataServiceFactory( context ) {
      @Override public Repository getRepository() {
        return supplier.get();
      }
    };
  }

  @Test
  public void testGetDataService() throws Exception {
    DataServiceFactory.setMetastoreSupplier( () -> metaStore );
    assertThat( factory.getDataServiceNames(), empty() );
    metaStoreUtil.save( dataService );
    metaStoreUtil.sync( transMeta, exceptionHandler );

    assertThat( factory.getDataServiceNames(), contains( DATA_SERVICE_NAME ) );
    assertThat( factory.getDataService( DATA_SERVICE_NAME ), validDataService() );
    assertThat( factory.getDataServices( exceptionHandler ), contains( validDataService() ) );

    verify( exceptionHandler, never() ).apply( any( Exception.class ) );
  }

  @Test
  public void testCreateClient() throws Exception {

    DataServiceFactory.setMetastoreSupplier( () -> metaStore );

    when( repositoryLocator.getRepository() ).thenReturn( repository );
    DataServiceResolver dataServiceResolver = new MetaStoreResolver( repositoryLocator, context );

    DataServiceClient client = new DataServiceClient( queryService, dataServiceResolver, Executors.newCachedThreadPool() );
    metaStoreUtil.save( dataService );
    metaStoreUtil.sync( transMeta, exceptionHandler );

    assertThat( client.getServiceInformation(), contains( hasProperty( "name", equalTo( DATA_SERVICE_NAME ) ) ) );
  }

  @Test
  public void testLocalMetaStore() throws Exception {
    DataServiceFactory.setMetastoreSupplier( () -> metaStore );

    assertThat( factory.getRepository(), sameInstance( repository ) );
    assertThat( factory.getMetaStore(), sameInstance( (IMetaStore) metaStore ) );

    when( supplier.get() ).thenReturn( null );
    assertThat( factory.getRepository(), nullValue() );
    assertThat( factory.getMetaStore(), sameInstance( metaStore ) );
  }

  @Test
  public void testCreateBuilderNonStreaming() throws Exception {
    when( dsMeta.isStreaming() ).thenReturn( false );

    factory = new DataServiceFactory( context ) {
      @Override public Repository getRepository() {
        return supplier.get();
      }
      @Override public DataServiceMeta getDataService( String serviceName, Repository repository,
                                                       IMetaStore metaStore ) {
        return dsMeta;
      }
    };

    factory.createBuilder( sql );

    verify( dsMeta, never() ).getRowLimit();
    verify( dsMeta, never() ).getTimeLimit();
  }

  @Test
  public void testCreateBuilderStreaming() throws Exception {
    when( dsMeta.isStreaming() ).thenReturn( true );

    factory = new DataServiceFactory( context ) {
      @Override public Repository getRepository() {
        return supplier.get();
      }
      @Override public DataServiceMeta getDataService( String serviceName, Repository repository,
                                                       IMetaStore metaStore ) {
        return dsMeta;
      }
    };

    factory.createBuilder( sql );

    verify( dsMeta ).getRowLimit();
    verify( dsMeta ).getTimeLimit();
  }
}
