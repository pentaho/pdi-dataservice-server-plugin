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


package org.pentaho.di.trans.dataservice.resolvers;

import com.google.common.base.Function;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by fcamara on 10/12/2016.
 */
@RunWith( MockitoJUnitRunner.StrictStubs.class)
public class DataServiceResolverDelegateTest {

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

  @Before
  public void setup() throws Exception {
    when( resolver.getDataServices( any() ) ).thenReturn( dataServices );
    when( resolver.getDataServices( DATA_SERVICE_NAME, logger ) ).thenReturn( dataServices );
    when( dataServices.toArray() ).thenReturn( new DataServiceMeta[] { dataServiceMeta } );
    when( resolver.getDataService( DATA_SERVICE_NAME ) ).thenReturn( dataServiceMeta );
    when( resolver.getDataServiceNames( DATA_SERVICE_NAME ) ).thenReturn( dataServicesName );
    when( resolver.getDataServiceNames() ).thenReturn( dataServicesName );
    when( resolver.createBuilder( sql ) ).thenReturn( builder );
    when( sql.getServiceName() ).thenReturn( DATA_SERVICE_NAME );

    dataServiceResolverDelegate = new DataServiceResolverDelegate();
    dataServiceResolverDelegate.addResolver( resolver );
  }

  @Test
  public void testGetDataServicesWithFunction() {
    List<DataServiceMeta> lDataServiceMeta = dataServiceResolverDelegate.getDataServices( logger );

    assertTrue( lDataServiceMeta.size() == 1 );
    verify( resolver ).getDataServices( logger );
  }

  @Test
  public void testGetDataServicesWithName() {
    List<DataServiceMeta> lDataServiceMeta = dataServiceResolverDelegate.getDataServices( DATA_SERVICE_NAME, logger );

    assertTrue( lDataServiceMeta.size() == 1 );
    verify( resolver ).getDataServices( DATA_SERVICE_NAME, logger );
  }

  @Test
  public void testGetDataServiceWithName() {
    DataServiceMeta returnDataServiceMeta = dataServiceResolverDelegate.getDataService( DATA_SERVICE_NAME );

    assertEquals( returnDataServiceMeta, dataServiceMeta );
    verify( resolver ).getDataService( DATA_SERVICE_NAME );
  }

  @Test
  public void testGetDataServiceWithNameRetNull() {
    when( resolver.getDataService( DATA_SERVICE_NAME ) ).thenReturn( null );
    DataServiceMeta returnDataServiceMeta = dataServiceResolverDelegate.getDataService( DATA_SERVICE_NAME );

    assertNull( returnDataServiceMeta );
    verify( resolver ).getDataService( DATA_SERVICE_NAME );
  }

  @Test
  public void testGetDataServiceNoResolvers() {
    DataServiceResolverDelegate
      nullDataServiceResolverDelegate =
      new DataServiceResolverDelegate();
    DataServiceMeta returnDataServiceMeta = nullDataServiceResolverDelegate.getDataService( DATA_SERVICE_NAME );

    assertNull( returnDataServiceMeta );
  }

  @Test
  public void testGetDataServiceNamesNoResolvers() {
    DataServiceResolverDelegate
      nullDataServiceResolverDelegate =
      new DataServiceResolverDelegate();
    List<String> names = nullDataServiceResolverDelegate.getDataServiceNames();

    assertEquals( names, Collections.emptyList() );
  }

  @Test
  public void testGetDataServiceNamesWName() {
    List<String> lDataServiceNames = dataServiceResolverDelegate.getDataServiceNames( DATA_SERVICE_NAME );

    assertTrue( lDataServiceNames.contains( DATA_SERVICE_NAME ) );
    verify( resolver ).getDataServiceNames( DATA_SERVICE_NAME );
  }

  @Test
  public void testGetDataServiceNames() {
    List<String> lDataServiceNames = dataServiceResolverDelegate.getDataServiceNames();

    assertTrue( lDataServiceNames.contains( DATA_SERVICE_NAME ) );
    verify( resolver ).getDataServiceNames();
  }

  @Test
  public void testCreateBuilder() throws KettleException {
    DataServiceExecutor.Builder returnBuilder = dataServiceResolverDelegate.createBuilder( sql );

    assertEquals( returnBuilder, builder );
    verify( resolver ).createBuilder( sql );
    verify( sql, never() ).getServiceName();
  }

  @Test
  public void testCreateBuilderExceptionDataServiceFailedSqlBuild() throws KettleException {
    when( resolver.createBuilder( sql ) ).thenReturn( null );
    thrown.expect( KettleException.class );
    thrown.expectMessage( ERROR_BUILDING_SQL );

    DataServiceExecutor.Builder returnBuilder = dataServiceResolverDelegate.createBuilder( sql );

    verify( resolver ).createBuilder( sql );
    verify( sql ).getServiceName();
  }

  @Test
  public void testCreateBuilderExceptionDataServiceNotFound() throws KettleException {
    when( resolver.createBuilder( sql ) ).thenReturn( null );
    when( resolver.getDataService( DATA_SERVICE_NAME ) ).thenReturn( null );
    thrown.expect( KettleException.class );
    thrown.expectMessage( DATA_SERVICE_NOT_FOUND );

    DataServiceExecutor.Builder returnBuilder = dataServiceResolverDelegate.createBuilder( sql );

    verify( resolver ).createBuilder( sql );
    verify( sql ).getServiceName();
  }
}
