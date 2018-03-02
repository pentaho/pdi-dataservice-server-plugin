/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.clients.Query;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author bmorrise nhudak
 */
@RunWith( MockitoJUnitRunner.class )
public class TransDataServletTest extends BaseServletTest {

  private static final String BAD_CONTEXT_PATH = "/badsql";
  private static final String CONTEXT_PATH = "/sql";
  private static final String HEADER_SQL = "SQL";
  private static final String HEADER_MAX_ROWS = "MaxRows";
  private static final String TEST_SQL_QUERY = "SELECT * FROM dataservice_test";
  private static final String TEST_LARGE_SQL_QUERY = TEST_SQL_QUERY + " /" + StringUtils.repeat( "*", 8000 ) + "/";
  private static final String DEBUG_TRANS_FILE = "debugtransfile";
  private static final String TEST_MAX_ROWS = "100";
  private static final String PARAM_DEBUG_TRANS = "debugtrans";
  private static final String SERVLET_STRING = "Get data from a data service";
  private static final String serviceTransUUID = UUID.randomUUID().toString();
  private static final String genTransUUID = UUID.randomUUID().toString();
  private static final String GEN_TRANS_XML = "<trans name=genTrans mock/>";

  @Rule public TemporaryFolder fs = new TemporaryFolder();

  private Trans serviceTrans;

  private TransMeta genTransMeta;

  private Trans genTrans;

  private TransDataServlet servlet;

  private File debugTrans;

  @Before
  public void setUp() throws Exception {
    when( context.getLogChannel() ).thenReturn( log );
    serviceTrans = new Trans( transMeta );
    serviceTrans.setContainerObjectId( serviceTransUUID );
    genTransMeta = createTransMeta( TEST_SQL_QUERY );
    genTrans = new Trans( genTransMeta );
    genTrans.setContainerObjectId( genTransUUID );

    doReturn( GEN_TRANS_XML ).when( genTransMeta ).getXML();

    servlet = new TransDataServlet( client );
    servlet.setJettyMode( true );
    servlet.setup( transformationMap, null, null, null );

    when( request.getContextPath() ).thenReturn( CONTEXT_PATH );
    debugTrans = fs.newFile( DEBUG_TRANS_FILE );
  }

  @Test
  public void testDoPut() throws Exception {
    headers.put( HEADER_MAX_ROWS, TEST_MAX_ROWS );
    headers.put( HEADER_SQL, TEST_SQL_QUERY );
    parameters.put( "PARAMETER_FOO", "BAR" );
    parameters.put( PARAM_DEBUG_TRANS, debugTrans.getPath() );

    Query query = mock( Query.class );
    doReturn( query )
      .when( client )
      .prepareQuery( TEST_SQL_QUERY, Integer.valueOf( TEST_MAX_ROWS ), ImmutableMap.of( "FOO", "BAR" ) );
    when( query.getTransList() ).thenReturn( ImmutableList.of( serviceTrans, genTrans ) );

    when( request.getMethod() ).thenReturn( "POST" );
    servlet.service( request, response );
    verify( logChannel, never() ).logError( anyString(), (Throwable) any() );

    verify( request, times( 1 ) ).getParameter( HEADER_SQL );
    verify( request, times( 1 ) ).getParameter( HEADER_MAX_ROWS );

    verify( response ).setStatus( HttpServletResponse.SC_OK );
    verify( response ).setContentType( "binary/jdbc" );
    verify( query ).writeTo( outputStream );

    verify( transformationMap ).addTransformation(
      eq( DATA_SERVICE_NAME ),
      eq( serviceTransUUID ),
      eq( serviceTrans ),
      (TransConfiguration) argThat( hasProperty( "transMeta", is( transMeta ) ) )
    );
    verify( transformationMap ).addTransformation(
      eq( TEST_SQL_QUERY ),
      eq( genTransUUID ),
      eq( genTrans ),
      (TransConfiguration) argThat( hasProperty( "transMeta", is( genTransMeta ) ) )
    );
    Files.readLines( debugTrans, Charsets.UTF_8 ).contains( GEN_TRANS_XML );
  }

  @Test
  public void testLargeSQLQuery() throws Exception {
    parameters.put( HEADER_MAX_ROWS, TEST_MAX_ROWS );
    parameters.put( HEADER_SQL, TEST_LARGE_SQL_QUERY );

    Query query = mock( Query.class );
    doReturn( query )
        .when( client )
        .prepareQuery( TEST_LARGE_SQL_QUERY, Integer.valueOf( TEST_MAX_ROWS ), ImmutableMap.<String, String>of() );
    when( query.getTransList() ).thenReturn( ImmutableList.of( serviceTrans, genTrans ) );

    when( request.getMethod() ).thenReturn( "POST" );
    servlet.service( request, response );
    verify( logChannel, never() ).logError( anyString(), (Throwable) any() );

    verify( request, never() ).getHeader( HEADER_SQL );
    verify( request, never() ).getHeader( HEADER_MAX_ROWS );
  }

  @Test
  public void testMultipleParamValues() throws IOException, ServletException {
    parameters.put( "PARAMETER_foo", "bar" );
    parameters.put( "PARAMETER_foo", "baz" );
    parameters.put( "SQL", "select * from dual" );
    servlet.service( request, response );
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass( String.class );
    verify( log ).logDetailed( captor.capture() );
    assertThat( captor.getValue(), containsString( "PARAMETER_foo" ) );
  }

  @Test
  public void testDoGetException() throws Exception {
    when( factory.createBuilder( any( SQL.class ) ) ).thenThrow( new MetaStoreException( "expected" ) );

    headers.put( HEADER_SQL, TEST_SQL_QUERY );
    servlet.service( request, response );

    verify( response ).sendError( HttpServletResponse.SC_BAD_REQUEST );
  }

  @Test
  public void testDoGetMissingSQL() throws Exception {
    headers.removeAll( HEADER_SQL );
    servlet.service( request, response );

    verify( response ).sendError( HttpServletResponse.SC_BAD_REQUEST );
    verify( client, times( 1 ) ).getLogChannel();
  }

  @Test
  public void testDoGetBadPath() throws Exception {
    when( request.getContextPath() ).thenReturn( BAD_CONTEXT_PATH );

    servlet.service( request, response );

    verify( response, never() ).setStatus( HttpServletResponse.SC_OK );
  }

  @Test
  public void testToString() {
    assertEquals( SERVLET_STRING, servlet.toString() );
  }

  @Test
  public void testGetService() {
    assertEquals( CONTEXT_PATH + " (" + SERVLET_STRING + ")", servlet.getService() );
  }

  @Test
  public void testGetContextPath() {
    assertEquals( CONTEXT_PATH, servlet.getContextPath() );
  }
}
