/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.www;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.BaseTest;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.www.SlaveServerConfig;
import org.pentaho.di.www.TransformationMap;
import org.pentaho.metastore.stores.delegate.DelegatingMetaStore;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bmorrise on 10/1/15.
 */
@RunWith( MockitoJUnitRunner.class )
public class TransDataServletTest extends BaseTest {

  private static final String BAD_CONTEXT_PATH = "/badsql";
  private static final String CONTEXT_PATH = "/sql";
  private static final String HEADER_SQL = "SQL";
  private static final String HEADER_MAX_ROWS = "MaxRows";
  private static final String TEST_SQL_QUERY = "SELECT * FROM dataservice_test";
  private static final String DEBUG_TRANS_FILE = "debugtransfile";
  private static final String TEST_DUMMY_SQL_QUERY = "SELECT 1";
  private static final String TEST_MAX_ROWS = "100";
  private static final String PARAM_DEBUG_TRANS = "debugtrans";
  private static final String SERVLET_STRING = "Transformation data service";

  @Mock
  private HttpServletRequest request;

  @Mock
  private HttpServletResponse response;

  @Mock
  private TransformationMap transformationMap;

  @Mock
  private SlaveServerConfig slaveServerConfig;

  @Mock
  private Repository repository;

  @Mock
  private DelegatingMetaStore metaStore;

  @Mock
  private DataServiceExecutor executor;

  @Mock
  private DataServiceExecutor.Builder builder;

  @Mock
  private ServletOutputStream outputStream;

  @Mock
  private Trans serviceTrans;

  @Mock
  private TransMeta genTransMeta;

  @Mock
  private Trans genTrans;

  @Mock
  private FileOutputStream fileOutputStream;

  @Mock
  private PrintWriter printWriter;

  private TransDataServlet servlet;

  @Before
  public void setUp() throws Exception {
    when( context.getDataServiceClient() ).thenReturn( client );

    servlet = new TransDataServlet( context );
    servlet.setJettyMode( true );
    servlet.setLog( logChannel );
    servlet.setup( transformationMap, null, null, null );

    when( request.getContextPath() ).thenReturn( CONTEXT_PATH );
    when( request.getHeader( HEADER_SQL ) ).thenReturn( TEST_SQL_QUERY );
    when( request.getHeader( HEADER_MAX_ROWS ) ).thenReturn( TEST_MAX_ROWS );
    when( request.getParameter( PARAM_DEBUG_TRANS ) ).thenReturn( DEBUG_TRANS_FILE );
    when( request.getParameterNames() ).thenReturn( Collections.enumeration( new HashSet() ) );
    when( response.getOutputStream() ).thenReturn( outputStream );
    when( transformationMap.getSlaveServerConfig() ).thenReturn( slaveServerConfig );
    when( slaveServerConfig.getRepository() ).thenReturn( repository );
    when( slaveServerConfig.getMetaStore() ).thenReturn( metaStore );
    when( client.buildExecutor( any( SQL.class ) )).thenReturn( builder );
    when( builder.parameters( anyMapOf( String.class, String.class ) ) ).thenReturn( builder );
    when( builder.rowLimit( Integer.valueOf( TEST_MAX_ROWS ) ) ).thenReturn( builder );
    when( builder.build() ).thenReturn( executor );
    when( executor.executeQuery( (DataOutputStream) any() ) ).thenReturn( executor );
    when( executor.getServiceTransMeta() ).thenReturn( transMeta );
    when( executor.getServiceTrans() ).thenReturn( serviceTrans );
    when( executor.getGenTransMeta() ).thenReturn( genTransMeta );
    when( executor.getGenTrans() ).thenReturn( genTrans );
    when( client.getDebugFileOutputStream( DEBUG_TRANS_FILE ) ).thenReturn( fileOutputStream );
    when( genTransMeta.getXML() ).thenReturn( "" );
  }

  @Test
  public void testDoPut() throws Exception {
    servlet.doPut( request, response );
    verifyRun();
  }

  @Test
  public void testDoGet() throws Exception {
    servlet.doGet( request, response );
    verifyRun();
  }

  @Test
  public void testDoGetException() throws Exception {
    when( client.buildExecutor( any( SQL.class ) ) ).thenThrow( new KettleException() );
    when( response.getWriter() ).thenReturn( printWriter );

    servlet.doGet( request, response );

    verify( response ).setStatus( HttpServletResponse.SC_BAD_REQUEST );
  }

  private void verifyRun() throws Exception {
    verify( response ).setStatus( HttpServletResponse.SC_OK );
    verify( response ).setContentType( "binary/jdbc" );
    verify( response ).setBufferSize( 10000 );
    verify( request ).getHeader( HEADER_SQL );
    verify( request ).getHeader( HEADER_MAX_ROWS );
    verify( request ).getParameter( PARAM_DEBUG_TRANS );
    verify( client ).setRepository( repository );
    verify( client ).setMetaStore( metaStore );
    verify( client ).buildExecutor( any( SQL.class ) );
    verify( builder ).parameters( anyMapOf( String.class, String.class ) );
    verify( builder ).rowLimit( Integer.valueOf( TEST_MAX_ROWS ) );
    verify( builder ).build();
    verify( executor ).executeQuery( (DataOutputStream) any() );
    verify( executor ).getServiceTransMeta();
    verify( executor ).getServiceTrans();
    verify( transformationMap, times( 2 ) )
        .addTransformation( anyString(), anyString(), any( Trans.class ), any( TransConfiguration.class ) );
    verify( executor ).getGenTransMeta();
    verify( executor ).getGenTrans();
    verify( client ).getDebugFileOutputStream( DEBUG_TRANS_FILE );
    verify( fileOutputStream ).close();
    verify( executor ).waitUntilFinished();
  }

  @Test
  public void testWriteDummyRow() throws Exception {
    when( request.getHeader( HEADER_SQL ) ).thenReturn( TEST_DUMMY_SQL_QUERY );

    servlet.doGet( request, response );

    verify( client ).writeDummyRow( any( SQL.class ), any( DataOutputStream.class ) );
    verify( executor, never() ).executeQuery( any( DataOutputStream.class ) );
  }

  @Test
  public void testDoGetBadPath() throws Exception {
    when( request.getContextPath() ).thenReturn( BAD_CONTEXT_PATH );

    servlet.doGet( request, response );

    verify( response, never() ).setStatus( HttpServletResponse.SC_OK );
  }

  @Test
  public void testGetParametersFromRequestHeader() throws Exception {
    Set<String> parameterNames = new HashSet<>();
    parameterNames.add( "PARAMETER_ONE" );
    parameterNames.add( "PARAMETER_TWO" );

    when( request.getParameterNames() ).thenReturn( Collections.enumeration( parameterNames ) );
    when( request.getParameter( "PARAMETER_ONE" ) ).thenReturn( "parameter1" );
    when( request.getParameter( "PARAMETER_TWO" ) ).thenReturn( "parameter2" );

    Map<String, String> parameters = TransDataServlet.getParametersFromRequestHeader( request );
    assertEquals( "parameter1", parameters.get( "ONE" ) );
    assertEquals( "parameter2", parameters.get( "TWO" ) );
  }

  @Test
  public void testToString() {
    assertEquals( SERVLET_STRING, servlet.toString() );
  }

  @Test
  public void testGetService() {
    assertEquals( CONTEXT_PATH + " ("+SERVLET_STRING+")", servlet.getService() );
  }

  @Test
  public void testGetContextPath() {
    assertEquals( CONTEXT_PATH, servlet.getContextPath() );
  }
}
