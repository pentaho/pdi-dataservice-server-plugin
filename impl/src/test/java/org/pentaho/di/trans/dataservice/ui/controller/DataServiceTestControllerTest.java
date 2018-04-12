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

package org.pentaho.di.trans.dataservice.ui.controller;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.parameters.NamedParams;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.core.sql.SQLField;
import org.pentaho.di.core.sql.SQLFields;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;
import org.pentaho.di.trans.dataservice.clients.AnnotationsQueryService;
import org.pentaho.di.trans.dataservice.clients.Query;
import org.pentaho.di.trans.dataservice.streaming.StreamServiceKey;
import org.pentaho.di.trans.dataservice.streaming.execution.StreamingServiceTransExecutor;
import org.pentaho.di.trans.dataservice.ui.DataServiceTestCallback;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceTestModel;
import org.pentaho.di.trans.step.RowListener;
import org.pentaho.ui.xul.XulComponent;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.components.XulLabel;
import org.pentaho.ui.xul.components.XulMenuList;
import org.pentaho.ui.xul.components.XulRadio;
import org.pentaho.ui.xul.components.XulTextbox;
import org.pentaho.ui.xul.containers.XulGroupbox;
import org.pentaho.ui.xul.dom.Document;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DataServiceTestControllerTest  {

  @Mock
  private DataServiceExecutor dataServiceExecutor;

  @Mock
  private DataServiceTestModel model;

  @Mock
  private DataServiceMeta dataService;

  @Mock
  private TransMeta transMeta;

  @Mock
  private RowMeta rowMeta;

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
  private XulRadio xulRadio;

  @Mock
  private XulLabel xulLabel;

  @Mock
  private XulGroupbox xulGroupbox;

  @Mock
  private DataServiceContext context;

  @Mock
  private AnnotationsQueryService annotationsQueryService;

  @Mock
  private Query annotationsQuery;

  @Mock
  private Query annotationsStreamingQuery;

  @Mock
  private StreamingServiceTransExecutor streamingExecution;

  @Mock
  private StreamServiceKey streamServiceKey;

  @Captor
  private ArgumentCaptor<String> queryCaptor;

  private DataServiceTestControllerTester dataServiceTestController;

  private static final String TEST_TABLE_NAME = "TestTable";
  private static final int VERIFY_TIMEOUT_MILLIS = 2000;
  private static final String TEST_ANNOTATIONS = "<annotations> \n</annotations>";
  private static final String MOCK_SQL = "show annotations from TestTable";
  private static final IDataServiceClientService.StreamingMode MOCK_WINDOW_MODE
    = IDataServiceClientService.StreamingMode.ROW_BASED;
  private static final long MOCK_WINDOW_SIZE = 1;
  private static final long MOCK_WINDOW_EVERY = 2L;
  private static final long MOCK_WINDOW_LIMIT = 3L;

  @Before
  public void initMocks() throws Exception {
    MockitoAnnotations.initMocks( this );

    when( dataService.getServiceTrans() ).thenReturn( transMeta );
    when( transMeta.realClone( false ) ).thenReturn( transMeta );

    doAnswer( new Answer<Void>() {
      @Override
      public Void answer( InvocationOnMock invocation ) throws Throwable {
        ( (OutputStream) invocation.getArguments()[0] ).write( TEST_ANNOTATIONS.getBytes() );
        return null;
      }
    } ).when( annotationsQuery ).writeTo( any( OutputStream.class ) );

    doAnswer( new Answer<Query>() {
      @Override
      public Query answer( InvocationOnMock invocation ) {
        String sql = (String) invocation.getArguments()[ 0 ];
        if ( null != sql && sql.startsWith( "show annotations from " ) ) {
          return annotationsQuery;
        }
        return null;
      }
    } ).when( annotationsQueryService ).prepareQuery( any( String.class ), anyInt(), any( Map.class ) );

    when( streamingExecution.getKey() ).thenReturn( streamServiceKey );
    when( context.getServiceTransExecutor( streamServiceKey ) ).thenReturn( streamingExecution );

    when( dataServiceExecutor.getServiceTrans() ).thenReturn( mock( Trans.class ) );
    when( dataServiceExecutor.getGenTrans() ).thenReturn( mock( Trans.class ) );
    when( dataServiceExecutor.getServiceTransMeta() ).thenReturn( transMeta );
    when( dataServiceExecutor.getServiceTrans().getLogChannel() ).thenReturn( mock( LogChannelInterface.class ) );
    when( dataServiceExecutor.getGenTrans().getLogChannel() ).thenReturn( mock( LogChannelInterface.class ) );
    when( dataServiceExecutor.getSql() ).thenReturn( mock( SQL.class ) );
    when( dataServiceExecutor.getSql().getSelectFields() ).thenReturn( mock( SQLFields.class ) );
    when( dataServiceExecutor.getSql().getRowMeta() ).thenReturn( new RowMeta() );
    when( dataServiceExecutor.getSql().getSelectFields().getFields() )
      .thenReturn( new ArrayList<SQLField>() );

    when( rowMeta.getValueMetaList() ).thenReturn( Collections.EMPTY_LIST );

    when( transMeta.listParameters() ).thenReturn( new String[] { "foo", "bar" } );
    when( transMeta.listVariables() ).thenReturn( new String[] { "varFoo", "varBar" } );
    when( transMeta.getStepFields( anyString() ) ).thenReturn( rowMeta );

    when( transMeta.getVariable( "varFoo" ) ).thenReturn( "varFooVal" );
    when( transMeta.getVariable( "varBar" ) ).thenReturn( "varBarVal" );
    when( transMeta.getParameterDefault( "foo" ) ).thenReturn( "fooVal" );
    when( transMeta.getParameterDefault( "bar" ) ).thenReturn( "barVal" );

    when( bindingFactory.createBinding( same( model ), anyString(), any( XulComponent.class ), anyString() ) ).thenReturn( binding );

    // mocks to deal with Xul multithreading.
    when( xulDomContainer.getDocumentRoot() ).thenReturn( document );
    doAnswer( new Answer() {
      @Override public Object answer( InvocationOnMock invocationOnMock ) throws Throwable {
        ( (Runnable) invocationOnMock.getArguments()[ 0 ] ).run();
        return null;
      }
    } ).when( document ).invokeLater( any( Runnable.class ) );
    when( dataService.getName() ).thenReturn( TEST_TABLE_NAME );
    when( dataService.isStreaming() ).thenReturn( false );

    when( model.getWindowMode() ).thenReturn( null );
    when( model.getWindowSize() ).thenReturn( 0L );
    when( model.getWindowEvery() ).thenReturn( 0L );
    when( model.getWindowLimit() ).thenReturn( 0L );

    dataServiceTestController = new DataServiceTestControllerTester();
    dataServiceTestController.setXulDomContainer( xulDomContainer );
    dataServiceTestController.setAnnotationsQueryService( annotationsQueryService );
  }

  @Test
  public void testInit() throws Exception {
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

    dataServiceTestController.init();

    verify( bindingFactory ).setDocument( document );
    verify( document, times( 21 ) ).getElementById( anyString() );
  }

  @Test
  public void defaultSqlUsesQuotedTableName() throws KettleException {
    verify( model ).setSql( queryCaptor.capture() );
    assertThat(
      queryCaptor.getValue(), containsString( "\"" +  TEST_TABLE_NAME + "\"" ) );
  }

  @Test
  public void logChannelsAreUpdatedInModelBeforeSqlExec() throws KettleException {
    dataServiceTestController.executeSql();

    InOrder inOrder = inOrder( model, dataServiceExecutor, callback );
    inOrder.verify( model ).setServiceTransLogChannel( dataServiceExecutor.getServiceTrans().getLogChannel() );
    inOrder.verify( model ).setGenTransLogChannel( dataServiceExecutor.getGenTrans().getLogChannel() );
    inOrder.verify( callback ).onLogChannelUpdate();
    inOrder.verify( dataServiceExecutor ).executeQuery( any( RowListener.class ) );
  }

  @Test
  public void errorAlertMessageBlankWhenNoError() throws KettleException {
    dataServiceTestController.executeSql();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass( String.class );
    // error message should be set to blank
    verify( model, times( 1 ) ).setAlertMessage( argument.capture() );
    assertTrue( argument.getValue().equals( "" ) );
  }

  @Test
  public void errorAlertMessageSetIfErrorInGenTrans() throws KettleException {
    when( model.isExecuting() ).thenReturn( true );
    when( dataServiceExecutor.getGenTrans().getErrors() ).thenReturn( 1 );
    when( dataServiceExecutor.getGenTrans().isFinishedOrStopped() )
      .thenReturn( true );
    dataServiceTestController.executeSql();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass( String.class );

    verify( model, timeout( VERIFY_TIMEOUT_MILLIS ).atLeast( 2 ) ).setAlertMessage( argument.capture() );
    assertTrue( argument.getValue().length() > 0 );
    assertEquals( "There were errors, review logs.", argument.getValue() );
  }

  @Test
  public void errorAlertMessageSetIfErrorInSvcTrans() throws KettleException {
    when( model.isExecuting() ).thenReturn( true );
    when( dataServiceExecutor.getServiceTrans().getErrors() ).thenReturn( 1 );
    when( dataServiceExecutor.getServiceTrans().isFinishedOrStopped() )
      .thenReturn( true );
    when( dataServiceExecutor.getGenTrans().isFinishedOrStopped() )
      .thenReturn( true );
    dataServiceTestController.executeSql();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass( String.class );
    verify( model, timeout( VERIFY_TIMEOUT_MILLIS ).atLeast( 2 ) ).setAlertMessage( argument.capture() );
    assertTrue( argument.getValue().length() > 0 );
    assertEquals( "There were errors, review logs.", argument.getValue() );
  }

  @Test
  public void hideStreamingTest() throws KettleException {
    assertTrue( dataServiceTestController.hideStreaming() );
  }

  @Test
  public void callbackNotifiedOnExecutionComplete() throws Exception {
    when( model.isExecuting() ).thenReturn( true );
    dataServiceTestController.executeSql();
    when( dataServiceExecutor.getServiceTrans().isFinishedOrStopped() )
      .thenReturn( true );
    when( dataServiceExecutor.getGenTrans().isFinishedOrStopped() )
      .thenReturn( true );
    verify( callback, timeout( VERIFY_TIMEOUT_MILLIS ).times( 1 ) ).onExecuteComplete();
  }

  @Test
  public void callbackNotifiedOnExecutionCompleteStreaming() throws Exception {
    when( model.isExecuting() ).thenReturn( true );
    dataServiceTestController.executeSql();
    when( dataService.isStreaming() ).thenReturn( true );
    when( dataServiceExecutor.getGenTrans().isFinished() )
      .thenReturn( true );
    verify( callback, timeout( VERIFY_TIMEOUT_MILLIS ).times( 1 ) ).onExecuteComplete();
  }

  @Test
  public void resultRowMetaIsUpdated() throws Exception {
    SQLField field = mock( SQLField.class );
    List<SQLField> fields = new ArrayList<>();
    fields.add( field );
    when( field.getField() ).thenReturn( "testFieldName" );
    when( dataServiceExecutor.getSql().getSelectFields().getFields() )
      .thenReturn( fields );
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "testFieldName" ) );
    when( dataServiceExecutor.getSql().getRowMeta() ).thenReturn( rowMeta );
    ArgumentCaptor<RowMetaInterface> argument = ArgumentCaptor.forClass( RowMetaInterface.class );
    dataServiceTestController.executeSql();
    verify( model, times( 1 ) ).setResultRowMeta( argument.capture() );
    assertThat( argument.getValue().getFieldNames(), equalTo( new String[] { "testFieldName" } ) );
  }

  @Test
  public void previousResultsAreClearedOnSqlExec() throws KettleException {
    dataServiceTestController.executeSql();
    verify( model, times( 1 ) ).clearResultRows();
  }

  @Test
  public void parametersAreResetOnClose() throws KettleException {
    // verifies that the parameter values captured when the controller
    // is initialized are reset on close, i.e. that params set during
    // use of the dialog do not leak.
    dataServiceTestController.initStartingParameterValues();
    dataServiceTestController.executeSql();
    verify( dataServiceExecutor.getServiceTrans(), never() ).prepareExecution( any( String[].class ) );
    dataServiceTestController.close();
    ArgumentCaptor<NamedParams> paramCaptor = ArgumentCaptor.forClass( NamedParams.class );
    verify( transMeta ).copyParametersFrom( paramCaptor.capture() );

    NamedParams params = paramCaptor.getValue();
    assertThat( Arrays.asList( params.listParameters() ), containsInAnyOrder( "foo", "bar" ) );
    assertThat( params.getParameterDefault( "foo" ), is( "fooVal" ) );
    assertThat( params.getParameterDefault( "bar" ), is( "barVal" ) );
  }

  @Test
  public void queryForAnnontations() throws KettleException, IOException {
    when( model.getSql() ).thenReturn( MOCK_SQL );
    dataServiceTestController.executeSql();

    ArgumentCaptor<RowMetaInterface> rowMetaCaptor = ArgumentCaptor.forClass( RowMetaInterface.class );
    verify( model, times( 2 ) ).setResultRowMeta( rowMetaCaptor.capture() );
    List<RowMetaInterface> capturedRowMeta = rowMetaCaptor.getAllValues();
    assertEquals( "annotations", capturedRowMeta.get( 1 ).getValueMeta( 0 ).getName() );

    ArgumentCaptor<Object[]> rowCaptor = ArgumentCaptor.forClass( Object[].class );
    verify( model, times( 1 ) ).addResultRow( rowCaptor.capture() );
    assertEquals( TEST_ANNOTATIONS, new String( (byte[]) rowCaptor.getValue()[0] ) );

    verify( callback, never() ).onLogChannelUpdate();
    verify( dataServiceExecutor, never() ).executeQuery();

    doThrow( new IOException() ).when( annotationsQuery ).writeTo( any( OutputStream.class ) );

    try {
      dataServiceTestController.executeSql();
      fail();
    } catch ( KettleException e ) {
      // Pass test
    } catch ( Exception e ) {
      fail();
    }
  }

  @Test
  public void streamingQueryForAnnontations() throws KettleException, IOException {
    when( model.getSql() ).thenReturn( MOCK_SQL );
    when( model.getWindowMode() ).thenReturn( MOCK_WINDOW_MODE );
    when( model.getWindowSize() ).thenReturn( MOCK_WINDOW_SIZE );
    when( model.getWindowEvery() ).thenReturn( MOCK_WINDOW_EVERY );
    when( model.getWindowLimit() ).thenReturn( MOCK_WINDOW_LIMIT );

    when( document.getElementById( "time-based-radio" ) ).thenReturn( xulRadio );
    when( dataService.isStreaming() ).thenReturn( true );

    when( annotationsQueryService.prepareQuery( MOCK_SQL, MOCK_WINDOW_MODE,
      MOCK_WINDOW_SIZE, MOCK_WINDOW_EVERY,
      MOCK_WINDOW_LIMIT, ImmutableMap.<String, String>of() ) ).thenReturn( annotationsStreamingQuery );

    doThrow( new IOException() ).when( annotationsStreamingQuery ).writeTo( any( OutputStream.class ) );

    try {
      dataServiceTestController.executeSql();
      fail();
    } catch ( KettleException e ) {
      // Pass test
    } catch ( Exception e ) {
      fail();
    }
  }

  @Test
  public void testPreviewOptimizations() throws KettleException {
    dataServiceTestController.previewQueries();

    verify( model ).clearOptimizationImpact();
  }

  @Test
  public void testPreviewOptimizationsTransRunning() throws KettleException {
    when( dataServiceExecutor.getServiceTrans().isRunning() ).thenReturn( true );

    dataServiceTestController.previewQueries();

    verify( model, times( 0 ) ).clearOptimizationImpact();
  }

  /**
   * Test class for purposes of injecting a mock DataServiceExecutor
   */
  class DataServiceTestControllerTester extends DataServiceTestController {

    private final DataServiceTestControllerTest test = DataServiceTestControllerTest.this;

    public DataServiceTestControllerTester() throws KettleException {
      super( model, dataService, bindingFactory, context );
      setCallback( test.callback );
      setXulDomContainer( test.xulDomContainer );
    }

    @Override
    protected DataServiceExecutor getNewDataServiceExecutor( boolean enableMetrics ) throws KettleException {
      return test.dataServiceExecutor;
    }
  }
}
