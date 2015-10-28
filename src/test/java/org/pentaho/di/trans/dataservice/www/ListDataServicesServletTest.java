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
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.clients.DataServiceClient;
import org.pentaho.di.trans.dataservice.jdbc.ThinServiceInformation;
import org.pentaho.di.www.SlaveServerConfig;
import org.pentaho.di.www.TransformationMap;
import org.pentaho.metastore.stores.delegate.DelegatingMetaStore;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bmorrise on 10/6/15.
 */
@RunWith( MockitoJUnitRunner.class )
public class ListDataServicesServletTest {

  private static final String SERVLET_STRING = "List data services";
  private static final String CONTEXT_PATH = "/listServices";
  private static final String SERVICE_NAME = "dataservice_test";

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
  private PrintWriter printWriter;

  @Mock
  private DataServiceClient client;

  @Mock
  private LogChannelInterface log;

  @Mock
  private ThinServiceInformation thinServiceInformation;

  @Mock
  private RowMetaInterface rowMetaInterface;

  @Mock
  private DataServiceContext context;

  private ListDataServicesServlet servlet;

  @Before
  public void setUp() throws Exception {
    when( context.getDataServiceClient() ).thenReturn( client );

    servlet = new ListDataServicesServlet( context );
    servlet.setJettyMode( true );
    servlet.setLog( log );
    servlet.setup( transformationMap, null, null, null );

    when( transformationMap.getSlaveServerConfig() ).thenReturn( slaveServerConfig );
    when( slaveServerConfig.getRepository() ).thenReturn( repository );
    when( slaveServerConfig.getMetaStore() ).thenReturn( metaStore );
    when( request.getContextPath() ).thenReturn( CONTEXT_PATH );
    when( response.getWriter() ).thenReturn( printWriter );

    when( thinServiceInformation.getName() ).thenReturn( SERVICE_NAME );
    when( thinServiceInformation.getServiceFields() ).thenReturn( rowMetaInterface );

    when( rowMetaInterface.getMetaXML() ).thenReturn( "" );

    List<ThinServiceInformation> serviceInformation = new ArrayList<>();
    serviceInformation.add( thinServiceInformation );
    when( client.getServiceInformation() ).thenReturn( serviceInformation );
  }

  @Test
  public void testDoGet() throws Exception {
    servlet.doGet( request, response );
    verifyRun();
  }

  @Test
  public void testDoPost() throws Exception {
    servlet.doGet( request, response );
    verifyRun();
  }

  private void verifyRun() throws Exception {
    verify( response ).setStatus( HttpServletResponse.SC_OK );
    verify( response ).setContentType( "text/xml" );
    verify( client ).setRepository( repository );
    verify( client ).setMetaStore( metaStore );
    verify( client ).getServiceInformation();
    verify( thinServiceInformation ).getName();
    verify( thinServiceInformation ).getServiceFields();
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
