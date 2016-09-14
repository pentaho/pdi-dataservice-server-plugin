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

package org.pentaho.di.trans.dataservice.clients;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.trans.dataservice.BaseTest;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.jdbc.ThinServiceInformation;
import org.pentaho.di.trans.dataservice.resolvers.DataServiceResolver;
import org.pentaho.di.trans.dataservice.resolvers.DataServiceResolverDelegate;
import org.pentaho.di.trans.dataservice.serialization.DataServiceFactory;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.osgi.metastore.locator.api.MetastoreLocator;
import org.pentaho.osgi.metastore.locator.api.MetastoreProvider;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.AllOf.allOf;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.pentaho.di.trans.dataservice.testing.answers.ReturnsSelf.RETURNS_SELF;

/**
 * @author bmorrise
 */
@RunWith( MockitoJUnitRunner.class )
public class DataServiceClientTest extends BaseTest {

  private static final String TEST_SQL_QUERY = "SELECT * FROM " + DATA_SERVICE_NAME;
  private static final int MAX_ROWS = 100;
  private DataServiceExecutor.Builder builder;

  @Mock DataServiceExecutor executor;
  @Mock RowMetaInterface rowMetaInterface;
  @Mock MetastoreLocator metastoreLocator;
  @Mock DataServiceResolver dataServiceResolver;
  @Mock Query.Service queryServiceDelegate;
  @Mock Query query;
  @Mock LogChannelInterface log;

  @Before
  public void setUp() throws Exception {
    when( dataServiceResolver.getDataService( DATA_SERVICE_NAME ) ).thenReturn( dataService );
    when( dataServiceResolver.getDataServices( anyString(), any( Function.class ) ) )
      .thenReturn( ImmutableList.of( dataService ) );
    when( dataServiceResolver.getDataServices( any( Function.class ) ) )
      .thenReturn( ImmutableList.of( dataService ) );

    builder = mock( DataServiceExecutor.Builder.class, RETURNS_SELF );
    when( dataServiceResolver.createBuilder( argThat( sql( TEST_SQL_QUERY ) ) ) ).thenReturn( builder );
    doReturn( executor ).when( builder ).build();
    when( executor.executeQuery( any( DataOutputStream.class ) ) ).thenReturn( executor );

    when( dataServiceResolver.createBuilder( argThat( sql( TEST_SQL_QUERY ) ) ) ).thenReturn( builder );
    client = new DataServiceClient( queryServiceDelegate, dataServiceResolver );
    client.setLogChannel( log );
  }

  @Test
  public void testQuery() throws Exception {
    when( queryServiceDelegate.prepareQuery( TEST_SQL_QUERY, MAX_ROWS, ImmutableMap.<String, String>of() ) )
      .thenReturn( query );

    assertNotNull( client.query( TEST_SQL_QUERY, MAX_ROWS ) );

    verify( queryServiceDelegate ).prepareQuery( TEST_SQL_QUERY, MAX_ROWS, ImmutableMap.<String, String>of() );
  }

  @Test
  public void testGetServiceInformation() throws Exception {
    when( transMeta.getStepFields( dataService.getStepname() ) ).thenReturn( rowMetaInterface );

    assertThat( client.getServiceInformation(), contains( allOf(
      hasProperty( "name", equalTo( DATA_SERVICE_NAME ) ),
      hasProperty( "serviceFields", equalTo( rowMetaInterface ) )
    ) ) );
    verify( transMeta ).activateParameters();

    when( transMeta.getStepFields( DATA_SERVICE_STEP ) ).thenThrow( new KettleStepException() );
    assertThat( client.getServiceInformation(), is( empty() ) );
  }

  @Test
  public void testGetServiceInformationByName() throws Exception {
    when( transMeta.getStepFields( dataService.getStepname() ) ).thenReturn( rowMetaInterface );

    ThinServiceInformation serviceInformation = client.getServiceInformation( DATA_SERVICE_NAME );
    verify( transMeta ).activateParameters();

    assertThat( serviceInformation.getName(), equalTo( DATA_SERVICE_NAME ) );
    assertThat( serviceInformation.getServiceFields(), equalTo( rowMetaInterface ) );
  }
}
