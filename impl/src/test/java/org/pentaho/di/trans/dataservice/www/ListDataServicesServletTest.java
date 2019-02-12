/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.dataservice.jdbc.ThinServiceInformation;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author bmorrise
 */
@RunWith( MockitoJUnitRunner.class )
public class ListDataServicesServletTest extends BaseServletTest {

  private static final String SERVLET_STRING = "List data services";
  private static final String CONTEXT_PATH = "/listServices";

  @Mock private RowMetaInterface rowMetaInterface;
  @Mock private SQLException sqlException;

  private ListDataServicesServlet servlet;
  private StringBuffer outputBuffer;

  @Before
  public void setUp() throws Exception {
    servlet = new ListDataServicesServlet( client );
    servlet.setJettyMode( true );
    servlet.setup( transformationMap, null, null, null );

    when( request.getContextPath() ).thenReturn( CONTEXT_PATH );

    StringWriter out = new StringWriter();
    ThinServiceInformation thinServiceInformation = new ThinServiceInformation( DATA_SERVICE_NAME, false, rowMetaInterface );
    when( response.getWriter() ).thenReturn( new PrintWriter( out ) );
    when( rowMetaInterface.getMetaXML() ).thenReturn( "<rowMeta mock/>" );
    when( client.getServiceInformation() ).thenReturn( ImmutableList.of( thinServiceInformation ) );
    outputBuffer = out.getBuffer();
  }

  @Test
  public void testDoGet() throws Exception {
    servlet.service( request, response );
    verifyRun();
  }

  @Test
  public void testDoGetWithRequestParameter() throws Exception {
    when( request.getParameter( anyString() ) ).thenReturn( "false" );
    servlet.service( request, response );
    verifyRun();
  }

  @Test
  public void testDoPost() throws Exception {
    when( request.getMethod() ).thenReturn( "POST" );
    servlet.service( request, response );
    verifyRun();
  }

  private void verifyRun() throws Exception {
    verify( response ).setStatus( HttpServletResponse.SC_OK );
    verify( response ).setContentType( "text/xml; charset=utf-8" );
    Pattern pattern = Pattern.compile( "<\\?xml version=\"1.0\" encoding=\"UTF-8\"\\?>\\s*<services>\\s*<service>\\s*<name>" + DATA_SERVICE_NAME + "<\\/name>\\s*<streaming>N</streaming>\\s*<rowMeta mock\\/>\\s*<\\/service>\\s*<\\/services>\\s*" );
    assertTrue( pattern.matcher( outputBuffer ).matches() );
  }

  @Test
  public void testFailure() throws SQLException, IOException {
    when( client.getServiceInformation() ).thenThrow( sqlException );
    when( carteRequest.respond( 500 ) ).thenReturn( carteResponse );

    servlet.handleRequest( carteRequest );
    verify( log ).logError( any( String.class ), any( SQLException.class ) );
    verify( carteResponse ).withMessage( any( String.class ) );
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

  @Test
  public void testRequestGetParameters() throws Exception {
    ImmutableMultimap.Builder<String, String> builder = ImmutableMultimap.builder();
    builder.put( "serviceName", "service1" );
    ThinServiceInformation thinServiceInformation = new ThinServiceInformation( DATA_SERVICE_NAME, false, rowMetaInterface );
    when( carteRequest.getParameters() ).thenReturn( builder.build().asMap() );
    when( client.getServiceInformation( anyString() ) ).thenReturn( thinServiceInformation );
    when( carteRequest.respond( 200 ) ).thenReturn( carteResponse );
    servlet.handleRequest( carteRequest );
    verify( client, times( 1 ) ).getServiceInformation( anyString() );
  }
}
