/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018-2024 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.ui.controller;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.clients.AnnotationsQueryService;
import org.pentaho.di.trans.dataservice.clients.Query;
import org.pentaho.di.trans.dataservice.streaming.execution.StreamingServiceTransExecutor;
import org.pentaho.di.trans.dataservice.ui.DataServiceTestCallback;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceTestModel;
import org.pentaho.ui.xul.XulComponent;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.components.XulButton;
import org.pentaho.ui.xul.components.XulLabel;
import org.pentaho.ui.xul.components.XulMenuList;
import org.pentaho.ui.xul.components.XulRadio;
import org.pentaho.ui.xul.components.XulTextbox;
import org.pentaho.ui.xul.containers.XulGroupbox;
import org.pentaho.ui.xul.dom.Document;

import java.io.OutputStream;
import java.util.Map;

import static junit.framework.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.StrictStubs.class)
public class StreamingDataServiceTestControllerTest {

  private DataServiceExecutor dataServiceExecutor;

  @Mock
  private DataServiceTestModel model;

  @Mock
  private DataServiceMeta streamingDataService;

  @Mock
  private TransMeta transMeta;

  @Mock
  private DataServiceTestCallback callback;

  @Mock
  private XulDomContainer xulDomContainer;

  @Mock
  private Document document;

  @Mock
  private BindingFactory bindingFactory;

  @Mock
  private Binding binding;

  @Mock
  private XulMenuList xulMenuList;

  @Mock
  private XulTextbox xulTextBox;

  @Mock
  private XulGroupbox xulGroupbox;

  @Mock
  private XulRadio xulRadio;

  @Mock
  private XulLabel xulLabel;

  @Mock
  private XulButton xulButton;

  @Mock
  private DataServiceContext context;

  @Mock
  private AnnotationsQueryService annotationsQueryService;

  @Mock
  private StreamingServiceTransExecutor serviceTransExecutor;

  @Mock
  private StreamingServiceTransExecutor streamingServiceTransExecutor;

  @Mock
  private Query annotationsQuery;

  private DataServiceTestControllerTester dataServiceTestController;

  private static final String TEST_TABLE_NAME = "Test Table";
  private static final int VERIFY_TIMEOUT_MILLIS = 2000;
  private static final String TEST_ANNOTATIONS = "<annotations> \n</annotations>";


  @Before
  public void initMocks() throws Exception {
    when( streamingDataService.getServiceTrans() ).thenReturn( transMeta );

    lenient().doAnswer( new Answer<Void>() {
      @Override
      public Void answer( InvocationOnMock invocation ) throws Throwable {
        ( (OutputStream) invocation.getArguments()[0] ).write( TEST_ANNOTATIONS.getBytes() );
        return null;
      }
    } ).when( annotationsQuery ).writeTo( any( OutputStream.class ) );

    lenient().doAnswer( new Answer<Query>() {
      @Override
      public Query answer( InvocationOnMock invocation ) {
        String sql = (String) invocation.getArguments()[ 0 ];
        if ( null != sql && sql.startsWith( "show annotations from " ) ) {
          return annotationsQuery;
        }
        return null;
      }
    } ).when( annotationsQueryService ).prepareQuery( any( String.class ), anyInt(), any( Map.class ) );

    when( transMeta.listParameters() ).thenReturn( new String[] { "foo", "bar" } );
    when( transMeta.getParameterDefault( "foo" ) ).thenReturn( "fooVal" );
    when( transMeta.getParameterDefault( "bar" ) ).thenReturn( "barVal" );

    when( bindingFactory.createBinding( same( model ), anyString(), any( XulComponent.class ), anyString() ) ).thenReturn( binding );

    // mocks to deal with Xul multithreading.
    when( xulDomContainer.getDocumentRoot() ).thenReturn( document );
    lenient().doAnswer( new Answer() {
      @Override public Object answer( InvocationOnMock invocationOnMock ) throws Throwable {
        ( (Runnable) invocationOnMock.getArguments()[ 0 ] ).run();
        return null;
      }
    } ).when( document ).invokeLater( any( Runnable.class ) );

    when( streamingDataService.getName() ).thenReturn( TEST_TABLE_NAME );
    when( streamingDataService.isStreaming() ).thenReturn( true );

    dataServiceTestController = new DataServiceTestControllerTester();
    dataServiceTestController.setXulDomContainer( xulDomContainer );
    dataServiceTestController.setAnnotationsQueryService( annotationsQueryService );
  }

  @Test
  public void testInitStreaming() throws Exception {
    when( document.getElementById( "log-levels" ) ).thenReturn( xulMenuList );
    when( document.getElementById( "sql-textbox" ) ).thenReturn( xulTextBox );
    when( document.getElementById( "maxrows-combo" ) ).thenReturn( xulMenuList );
    when( document.getElementById( "streaming-groupbox" ) ).thenReturn( xulGroupbox );
    when( document.getElementById( "time-based-radio" ) ).thenReturn( xulRadio );
    when( document.getElementById( "row-based-radio" ) ).thenReturn( xulRadio );
    when( document.getElementById( "window-size" ) ).thenReturn( xulTextBox );
    when( document.getElementById( "window-every" ) ).thenReturn( xulTextBox );
    when( document.getElementById( "window-limit" ) ).thenReturn( xulTextBox );
    when( document.getElementById( "window-size-time-unit" ) ).thenReturn( xulLabel );
    when( document.getElementById( "window-every-time-unit" ) ).thenReturn( xulLabel );
    when( document.getElementById( "window-limit-time-unit" ) ).thenReturn( xulLabel );
    when( document.getElementById( "window-size-row-unit" ) ).thenReturn( xulLabel );
    when( document.getElementById( "window-every-row-unit" ) ).thenReturn( xulLabel );
    when( document.getElementById( "window-limit-row-unit" ) ).thenReturn( xulLabel );
    when( document.getElementById( "preview-opt-btn" ) ).thenReturn( xulButton );
    when( document.getElementById( "exec-sql-btn" ) ).thenReturn( xulButton );
    when( document.getElementById( "error-alert" ) ).thenReturn( xulLabel );
    when( document.getElementById( "optimization-impact-info" ) ).thenReturn( xulTextBox );

    dataServiceTestController.init();

    verify( bindingFactory ).setDocument( document );
    verify( document, times( 34 ) ).getElementById( anyString() );
  }

  @Test
  public void dontHideStreamingTest() throws KettleException {
    assertFalse( dataServiceTestController.hideStreaming() );
  }

  /**
   * Test class for purposes of injecting a mock DataServiceExecutor
   */
  class DataServiceTestControllerTester extends DataServiceTestController {

    private final StreamingDataServiceTestControllerTest test = StreamingDataServiceTestControllerTest.this;

    public DataServiceTestControllerTester() throws KettleException {
      super( model, streamingDataService, bindingFactory, context );
      setCallback( test.callback );
      setXulDomContainer( test.xulDomContainer );
    }

    @Override
    protected DataServiceExecutor getNewDataServiceExecutor( boolean enableMetrics ) throws KettleException {
      return test.dataServiceExecutor;
    }
  }
}
