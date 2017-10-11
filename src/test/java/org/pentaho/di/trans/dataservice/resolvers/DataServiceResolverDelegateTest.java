/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.resolvers;

import com.google.common.base.Function;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by fcamara on 10/12/2016.
 */
@RunWith( MockitoJUnitRunner.class ) public class DataServiceResolverDelegateTest {

  private final String DATA_SERVICE_NAME = "DS_TEST";
  private final String DATA_SERVICE_NOT_FOUND = "Data Service " + DATA_SERVICE_NAME + " was not found";
  private final String ERROR_BUILDING_SQL = "Error when creating builder for sql query";

  private DataServiceResolverDelegate dataServiceResolverDelegate;
  private List<String> dataServicesName = Arrays.asList( DATA_SERVICE_NAME );
  private List<DataServiceResolver> resolvers;

  @Mock private DataServiceResolver resolver;
  @Mock private List<DataServiceMeta> dataServices;
  @Mock private DataServiceMeta dataServiceMeta;
  @Mock private Function<Exception, Void> logger;
  @Mock private DataServiceExecutor.Builder builder;
  @Mock private SQL sql;

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Before public void setup() throws Exception {
    when( resolver.getDataServices( any() ) ).thenReturn( dataServices );
    when( resolver.getDataServices( DATA_SERVICE_NAME, logger ) ).thenReturn( dataServices );
    when( dataServices.toArray() ).thenReturn( new DataServiceMeta[] { dataServiceMeta } );
    when( resolver.getDataService( DATA_SERVICE_NAME ) ).thenReturn( dataServiceMeta );
    when( resolver.getDataServiceNames( DATA_SERVICE_NAME ) ).thenReturn( dataServicesName );
    when( resolver.getDataServiceNames() ).thenReturn( dataServicesName );
    when( resolver.createBuilder( sql ) ).thenReturn( builder );
    when( sql.getServiceName() ).thenReturn( DATA_SERVICE_NAME );

    when( dataServices.get( 0 ) ).thenReturn( dataServiceMeta );

    resolvers = Arrays.asList( resolver );
    dataServiceResolverDelegate = new DataServiceResolverDelegate( resolvers );
  }

  @Test public void testGetDataServicesWithFunction() {
    List<DataServiceMeta> lDataServiceMeta = dataServiceResolverDelegate.getDataServices( logger );

    assertTrue( lDataServiceMeta.size() == 1 );
    verify( resolver ).getDataServices( logger );
  }

  @Test public void testGetDataServicesWithName() {
    List<DataServiceMeta> lDataServiceMeta = dataServiceResolverDelegate.getDataServices( DATA_SERVICE_NAME, logger );

    assertTrue( lDataServiceMeta.size() == 1 );
    verify( resolver ).getDataServices( DATA_SERVICE_NAME, logger );
  }

  @Test public void testGetDataServiceWithName() {
    DataServiceMeta returnDataServiceMeta = dataServiceResolverDelegate.getDataService( DATA_SERVICE_NAME );

    assertEquals( returnDataServiceMeta, dataServiceMeta );
    verify( resolver ).getDataService( DATA_SERVICE_NAME );
  }

  @Test public void testGetDataServiceWithNameRetNull() {
    when( resolver.getDataService( DATA_SERVICE_NAME ) ).thenReturn( null );
    DataServiceMeta returnDataServiceMeta = dataServiceResolverDelegate.getDataService( DATA_SERVICE_NAME );

    assertNull( returnDataServiceMeta );
    verify( resolver ).getDataService( DATA_SERVICE_NAME );
  }

  @Test public void testGetDataServiceNoResolvers() {
    DataServiceResolverDelegate
        nullDataServiceResolverDelegate =
        new DataServiceResolverDelegate( new ArrayList<DataServiceResolver>() );
    DataServiceMeta returnDataServiceMeta = nullDataServiceResolverDelegate.getDataService( DATA_SERVICE_NAME );

    assertNull( returnDataServiceMeta );
  }

  @Test public void testGetDataServiceNamesNoResolvers() {
    DataServiceResolverDelegate
        nullDataServiceResolverDelegate =
        new DataServiceResolverDelegate( Collections.emptyList() );
    List<String> names = nullDataServiceResolverDelegate.getDataServiceNames();

    assertEquals( names, Collections.emptyList() );
  }

  @Test public void testGetDataServiceNamesWName() {
    List<String> lDataServiceNames = dataServiceResolverDelegate.getDataServiceNames( DATA_SERVICE_NAME );

    assertTrue( lDataServiceNames.contains( DATA_SERVICE_NAME ) );
    verify( resolver ).getDataServiceNames( DATA_SERVICE_NAME );
  }

  @Test public void testGetDataServiceNames() {
    List<String> lDataServiceNames = dataServiceResolverDelegate.getDataServiceNames();

    assertTrue( lDataServiceNames.contains( DATA_SERVICE_NAME ) );
    verify( resolver ).getDataServiceNames();
  }

  @Test public void testCreateBuilder() throws KettleException {
    DataServiceExecutor.Builder returnBuilder = dataServiceResolverDelegate.createBuilder( sql );

    assertEquals( returnBuilder, builder );
    verify( resolver ).createBuilder( sql );
    verify( sql, never() ).getServiceName();
  }

  @Test public void testCreateBuilderExceptionDataServiceFailedSqlBuild() throws KettleException {
    when( resolver.createBuilder( sql ) ).thenReturn( null );
    thrown.expect( KettleException.class );
    thrown.expectMessage( ERROR_BUILDING_SQL );

    DataServiceExecutor.Builder returnBuilder = dataServiceResolverDelegate.createBuilder( sql );

    verify( resolver ).createBuilder( sql );
    verify( sql ).getServiceName();
  }

  @Test public void testCreateBuilderExceptionDataServiceNotFound() throws KettleException {
    when( resolver.createBuilder( sql ) ).thenReturn( null );
    when( resolver.getDataService( DATA_SERVICE_NAME ) ).thenReturn( null );
    thrown.expect( KettleException.class );
    thrown.expectMessage( DATA_SERVICE_NOT_FOUND );

    DataServiceExecutor.Builder returnBuilder = dataServiceResolverDelegate.createBuilder( sql );

    verify( resolver ).createBuilder( sql );
    verify( sql ).getServiceName();
  }
}
