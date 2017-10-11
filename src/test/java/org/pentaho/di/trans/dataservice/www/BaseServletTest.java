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

package org.pentaho.di.trans.dataservice.www;

import com.google.common.base.Supplier;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.dataservice.BaseTest;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.clients.DataServiceClient;
import org.pentaho.di.trans.dataservice.serialization.DataServiceFactory;
import org.pentaho.di.www.CarteRequestHandler;
import org.pentaho.di.www.SlaveServerConfig;
import org.pentaho.di.www.TransformationMap;
import org.pentaho.metastore.api.IMetaStore;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Set;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */
public abstract class BaseServletTest extends BaseTest {
  @Mock HttpServletRequest request;
  @Mock HttpServletResponse response;
  @Mock CarteRequestHandler.CarteRequest carteRequest;
  @Mock CarteRequestHandler.CarteResponse carteResponse;
  @Mock LogChannelInterface log;
  @Mock TransformationMap transformationMap;
  @Mock Repository repository;
  @Mock ServletOutputStream outputStream;
  @Mock PrintWriter printWriter;
  @Mock SlaveServerConfig slaveServerConfig;
  @Mock DataServiceFactory factory;

  SetMultimap<String, String> headers;
  SetMultimap<String, String> parameters;

  @Before
  public void setupRequest() throws Exception {
    context = mock( DataServiceContext.class );
    when( context.getLogChannel() ).thenReturn( log );

    headers = HashMultimap.create();
    when( request.getHeaderNames() ).then( new Answer<Enumeration>() {
      @Override public Enumeration answer( InvocationOnMock invocation ) throws Throwable {
        return Collections.enumeration( headers.keySet() );
      }
    } );
    when( request.getHeaders( anyString() ) ).then( new Answer<Enumeration>() {
      @Override public Enumeration answer( InvocationOnMock invocation ) throws Throwable {
        return Collections.enumeration( headers.get( (String) invocation.getArguments()[0] ) );
      }
    } );
    when( request.getHeader( anyString() ) ).then( new Answer<String>() {
      @Override public String answer( InvocationOnMock invocation ) throws Throwable {
        Iterator<String> value = headers.get( (String) invocation.getArguments()[0] ).iterator();
        return value.hasNext() ? value.next() : null;
      }
    } );

    parameters = HashMultimap.create();
    when( request.getParameterNames() ).then( new Answer<Enumeration>() {
      @Override public Enumeration answer( InvocationOnMock invocation ) throws Throwable {
        return Collections.enumeration( parameters.keySet() );
      }
    } );
    when( request.getParameter( anyString() ) ).then( new Answer<String>() {
      @Override public String answer( InvocationOnMock invocation ) throws Throwable {
        Iterator<String> value = parameters.get( (String) invocation.getArguments()[0] ).iterator();
        return value.hasNext() ? value.next() : null;
      }
    } );
    when( request.getParameterValues( anyString() ) ).then( new Answer<String[]>() {
      @Override public String[] answer( InvocationOnMock invocation ) throws Throwable {
        Set<String> values = parameters.get( (String) invocation.getArguments()[0] );
        return values.toArray( new String[values.size()] );
      }
    } );

    when( request.getMethod() ).thenReturn( "GET" );

    when( response.getOutputStream() ).thenReturn( outputStream );
    when( response.getWriter() ).thenReturn( printWriter );
    when( transformationMap.getSlaveServerConfig() ).thenReturn( slaveServerConfig );
    when( slaveServerConfig.getRepository() ).thenReturn( repository );

    when( factory.getContext() ).thenReturn( context );
    when( factory.getStepCache() ).thenReturn( cache );
    when( factory.getLogChannel() ).thenReturn( logChannel );
    when( client.getLogChannel() ).thenReturn( log );
  }

  @After
  @SuppressWarnings( "deprecation" )
  public void tearDown() throws Exception {
    verify( client, never() ).setRepository( (Repository) any() );
    verify( client, never() ).setMetaStore( (IMetaStore) any() );
  }

  public class ValidRepositorySupplier extends ArgumentMatcher<Supplier<Repository>> {
    @Override public boolean matches( Object argument ) {
      return argument instanceof Supplier && ( (Supplier) argument ).get() == repository;
    }
  }
}
