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

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import javax.servlet.http.HttpServletResponse;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.util.UUID;

import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.pentaho.di.trans.dataservice.testing.answers.ReturnsSelf.RETURNS_SELF;

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
  private static final String DEBUG_TRANS_FILE = "debugtransfile";
  private static final String TEST_DUMMY_SQL_QUERY = "SELECT 1";
  private static final String TEST_MAX_ROWS = "100";
  private static final String PARAM_DEBUG_TRANS = "debugtrans";
  private static final String SERVLET_STRING = "Transformation data service";
  private static final String serviceTransUUID = UUID.randomUUID().toString();
  private static final String genTransUUID = UUID.randomUUID().toString();
  private static final String GEN_TRANS_XML = "<trans name=genTrans mock/>";

  @Rule public TemporaryFolder fs = new TemporaryFolder();

  @Mock
  private DataServiceExecutor executor;

  private Trans serviceTrans;

  private TransMeta genTransMeta;

  private Trans genTrans;

  private TransDataServlet servlet;

  private File debugTrans;

  @Before
  public void setUp() throws Exception {
    serviceTrans = new Trans( transMeta );
    serviceTrans.setContainerObjectId( serviceTransUUID );
    genTransMeta = createTransMeta( TEST_SQL_QUERY );
    genTrans = new Trans( genTransMeta );
    genTrans.setContainerObjectId( genTransUUID );

    doReturn( GEN_TRANS_XML ).when( genTransMeta ).getXML();

    when( executor.getServiceTrans() ).thenReturn( serviceTrans );
    when( executor.getServiceTransMeta() ).thenReturn( transMeta );
    when( executor.getGenTrans() ).thenReturn( genTrans );
    when( executor.getGenTransMeta() ).thenReturn( genTransMeta );

    servlet = new TransDataServlet( context );
    servlet.setJettyMode( true );
    servlet.setLog( logChannel );
    servlet.setup( transformationMap, null, null, null );

    verify( context ).createClient( argThat( new ValidRepositorySupplier() ) );

    when( request.getContextPath() ).thenReturn( CONTEXT_PATH );
    debugTrans = fs.newFile( DEBUG_TRANS_FILE );
  }

  @Test
  public void testDoPut() throws Exception {
    DataServiceExecutor.Builder builder = mock( DataServiceExecutor.Builder.class, RETURNS_SELF );

    when( factory.createBuilder( argThat( sql( TEST_SQL_QUERY ) ) ) ).thenReturn( builder );
    when( builder.build() ).thenReturn( executor );
    when( executor.executeQuery( (DataOutputStream) any() ) ).then( new Answer<DataServiceExecutor>() {
      @Override public DataServiceExecutor answer( InvocationOnMock invocation ) throws Throwable {
        ( (DataOutput) invocation.getArguments()[0] ).write( 32 );
        return executor;
      }
    } );

    headers.put( HEADER_MAX_ROWS, TEST_MAX_ROWS );
    headers.put( HEADER_SQL, TEST_SQL_QUERY );
    parameters.put( "PARAMETER_FOO", "BAR" );
    parameters.put( PARAM_DEBUG_TRANS, debugTrans.getPath() );

    when( request.getMethod() ).thenReturn( "PUT" );
    servlet.service( request, response );
    verify( logChannel, never() ).logError( anyString(), (Throwable) any() );

    verify( response ).setStatus( HttpServletResponse.SC_OK );
    verify( response ).setContentType( "binary/jdbc" );
    verify( builder ).parameters( ImmutableMap.of( "FOO", "BAR" ) );
    verify( builder ).rowLimit( Integer.valueOf( TEST_MAX_ROWS ) );
    verify( outputStream ).write( 32 );

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
    verify( executor ).waitUntilFinished();
    Files.readLines( debugTrans, Charsets.UTF_8 ).contains( GEN_TRANS_XML );
  }

  @Test
  public void testDoGetException() throws Exception {
    when( factory.createBuilder( any( SQL.class ) ) ).thenThrow( new MetaStoreException( "expected" ) );

    headers.put( HEADER_SQL, TEST_SQL_QUERY );
    servlet.service( request, response );

    verify( response ).setStatus( HttpServletResponse.SC_BAD_REQUEST );
  }

  @Test
  public void testDoGetMissingSQL() throws Exception {
    headers.removeAll( HEADER_SQL );
    servlet.service( request, response );

    verify( response ).setStatus( HttpServletResponse.SC_BAD_REQUEST );
    verifyZeroInteractions( client );
  }

  @Test
  public void testWriteDummyRow() throws Exception {
    when( request.getHeader( HEADER_SQL ) ).thenReturn( TEST_DUMMY_SQL_QUERY );

    servlet.service( request, response );

    verify( client ).writeDummyRow( any( SQL.class ), any( DataOutputStream.class ) );
  }

  @Test
  public void testDoGetBadPath() throws Exception {
    when( request.getContextPath() ).thenReturn( BAD_CONTEXT_PATH );

    servlet.service( request, response );

    verify( response, never() ).setStatus( HttpServletResponse.SC_OK );
  }

  @Test
  public void testExecutorErrors() throws Exception {
    DataServiceExecutor.Builder builder = mock( DataServiceExecutor.Builder.class, RETURNS_SELF );

    when( factory.createBuilder( argThat( sql( TEST_SQL_QUERY ) ) ) ).thenReturn( builder );
    when( builder.build() ).thenReturn( executor );
    when( executor.executeQuery( (DataOutputStream) any() ) ).then( new Answer<DataServiceExecutor>() {
      @Override public DataServiceExecutor answer( InvocationOnMock invocation ) throws Throwable {
        ( (DataOutput) invocation.getArguments()[0] ).write( 32 );
        return executor;
      }
    } );

    headers.put( HEADER_SQL, TEST_SQL_QUERY );
    when( executor.hasErrors() ).thenReturn( true );
    servlet.service( request, response );
    verify( logChannel ).logError( anyString(), (Throwable) any() );
    verify( response ).setStatus( HttpServletResponse.SC_BAD_REQUEST );
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
