/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2022 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.clients;

import com.google.common.collect.ImmutableList;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;
import org.pentaho.di.trans.dataservice.resolvers.DataServiceResolver;
import org.pentaho.di.trans.dataservice.serialization.DataServiceFactory;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.locator.api.MetastoreLocator;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link ExecutorQueryService} test class
 */
@RunWith( MockitoJUnitRunner.class )
public class ExecutorQueryServiceTest {
  private Query result;
  private int rowLimit = 5432;
  private SQL sql;
  private HashMap<String, String> parameters = new HashMap<>();
  private ExecutorQueryService executorQueryService;

  @Mock private DataServiceFactory factory;
  @Mock private DataServiceContext context;
  @Mock private DataServiceResolver dataServiceResolver;
  @Mock private DataServiceExecutor dataServiceExecutor;
  @Mock private Trans serviceTrans;
  @Mock private Trans genTrans;
  @Mock private DataServiceExecutor.Builder builder;
  @Mock private IMetaStore metastore;
  @Mock private MetastoreLocator metastoreLocator;
  @Mock private DataOutputStream dataOutputStream;
  @Mock private OutputStream outputStream;

  @Before
  public void setup() throws Exception {
    rowLimit = 5432;
    parameters = new HashMap<>();
    sql = new SQL( "select field from table" );

    when( context.getMetaStoreUtil() ).thenReturn( factory );
    when( metastoreLocator.getMetastore() ).thenReturn( metastore );

    executorQueryService = new ExecutorQueryService( dataServiceResolver, metastoreLocator );
    when( dataServiceResolver.createBuilder( argThat( matchesSql( sql ) ) ) ).thenReturn( builder );
    when( factory.getMetaStore() ).thenReturn( metastore );

    when( builder.rowLimit( rowLimit ) ).thenReturn( builder );
    when( builder.parameters( parameters ) ).thenReturn( builder );
    when( builder.metastore( metastore ) ).thenReturn( builder );
    when( builder.windowMode( any( IDataServiceClientService.StreamingMode.class ) ) ).thenReturn( builder );
    when( builder.windowSize( anyLong() ) ).thenReturn( builder );
    when( builder.windowEvery( anyLong() ) ).thenReturn( builder );
    when( builder.windowLimit( anyLong() ) ).thenReturn( builder );
    when( builder.rowLimit( anyInt() ) ).thenReturn( builder );
    when( builder.build() ).thenReturn( dataServiceExecutor );
    when( dataServiceExecutor.getServiceTrans() ).thenReturn( serviceTrans );
    when( dataServiceExecutor.getGenTrans() ).thenReturn( genTrans );
  }

  @Test
  public void testQueryBuildsWithMetastore() throws Exception {
    result = executorQueryService.prepareQuery( sql.getSqlString(), rowLimit, parameters );

    assertEquals( ImmutableList.of( serviceTrans, genTrans ), result.getTransList() );
    verify( builder ).rowLimit( rowLimit );
    verify( builder ).parameters( parameters );
    verify( builder ).metastore( metastore );
  }

  @Test
  public void testQueryBuildsWithWindowRowBased() throws Exception {
    result = executorQueryService.prepareQuery( sql.getSqlString(), IDataServiceClientService.StreamingMode.ROW_BASED,
      10, 1, 15000, parameters );

    assertEquals( ImmutableList.of( serviceTrans, genTrans ), result.getTransList() );
    verify( builder ).rowLimit( 0 );
    verify( builder ).parameters( parameters );
    verify( builder ).metastore( metastore );
    verify( builder ).windowLimit( 15000 );
    verify( builder ).windowSize( 10 );
    verify( builder ).windowEvery( 1 );
    verify( builder ).windowMode( IDataServiceClientService.StreamingMode.ROW_BASED );
  }

  @Test
  public void testQueryBuildsWithWindowTimeBased() throws Exception {
    result = executorQueryService.prepareQuery( sql.getSqlString(), IDataServiceClientService.StreamingMode.TIME_BASED,
      100, 10, 22000, parameters );

    assertEquals( ImmutableList.of( serviceTrans, genTrans ), result.getTransList() );
    verify( builder ).rowLimit( 0 );
    verify( builder ).parameters( parameters );
    verify( builder ).metastore( metastore );
    verify( builder ).windowLimit( 22000 );
    verify( builder ).windowSize( 100 );
    verify( builder ).windowEvery( 10 );
    verify( builder ).windowMode( IDataServiceClientService.StreamingMode.TIME_BASED );
  }

  @Test
  public void testAsDataOutputStream() throws IOException {
    assertSame( dataOutputStream, ExecutorQueryService.asDataOutputStream( dataOutputStream ) );
    DataOutputStream out = ExecutorQueryService.asDataOutputStream( outputStream );
    out.write( 1 );
    verify( outputStream ).write( 1 );
  }

  @Test
  public void testExecutorQueryInnerClass() throws Exception {
    SQL sql = new SQL( "select field from table" );
    ExecutorQueryService executorQueryService = new ExecutorQueryService( dataServiceResolver, metastoreLocator );
    Query executorQuery = executorQueryService.prepareQuery( sql.getSqlString(), rowLimit, parameters );

    DataOutputStream dataOutputStreamMock = mock( DataOutputStream.class );
    doReturn( dataServiceExecutor ).when( dataServiceExecutor ).executeQuery( dataOutputStreamMock );
    executorQuery.writeTo( dataOutputStreamMock );
    verify( dataServiceExecutor, times( 1 ) ).waitUntilFinished();
  }

  @Test
  public void testExecutorQueryInnerClassNullExecutor() throws Exception {
    SQL sql = new SQL( "select field from table" );
    ExecutorQueryService executorQueryService = new ExecutorQueryService( dataServiceResolver, metastoreLocator );
    Query executorQuery = executorQueryService.prepareQuery( sql.getSqlString(), rowLimit, parameters );

    DataOutputStream dataOutputStreamMock = mock( DataOutputStream.class );
    doReturn( null ).when( dataServiceExecutor ).executeQuery( dataOutputStreamMock );
    executorQuery.writeTo( dataOutputStreamMock );
    verify( dataServiceExecutor, times( 0 ) ).waitUntilFinished();
  }

  private Matcher<SQL> matchesSql( final SQL sql ) {
    return new BaseMatcher<SQL>() {
      @Override public boolean matches( final Object o ) {
        return ( (SQL) o ).getSqlString().equals( sql.getSqlString() );
      }

      @Override public void describeTo( final Description description ) {

      }
    };
  }
}
