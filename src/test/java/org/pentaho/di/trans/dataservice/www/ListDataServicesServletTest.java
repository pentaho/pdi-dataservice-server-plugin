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

package org.pentaho.di.trans.dataservice.www;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.dataservice.clients.DataServiceClient;
import org.pentaho.di.trans.dataservice.jdbc.ThinServiceInformation;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
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
    ThinServiceInformation thinServiceInformation = new ThinServiceInformation( DATA_SERVICE_NAME, rowMetaInterface );
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
  public void testDoPost() throws Exception {
    when( request.getMethod() ).thenReturn( "POST" );
    servlet.service( request, response );
    verifyRun();
  }

  private void verifyRun() throws Exception {
    verify( response ).setStatus( HttpServletResponse.SC_OK );
    verify( response ).setContentType( "text/xml" );
    List<String> outputLines = Splitter.on( '\n' ).omitEmptyStrings().splitToList( outputBuffer );
    assertThat( outputLines, equalTo( (List<String>) ImmutableList.of(
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
      "<services>",
      "<service>",
      "<name>" + DATA_SERVICE_NAME + "</name>",
      "<rowMeta mock/>",
      "</service>",
      "</services>"
    ) ) );
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
}
