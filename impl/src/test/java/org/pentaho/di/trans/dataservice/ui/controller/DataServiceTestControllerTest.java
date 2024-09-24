/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package org.pentaho.di.trans.dataservice.ui.controller;

import com.google.common.collect.ImmutableMap;
import io.reactivex.Observer;
import io.reactivex.disposables.Disposable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.parameters.NamedParams;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaInteger;
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

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.StrictStubs.class)
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
  private XulButton xulButton;

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

  @Captor
  private ArgumentCaptor<Observer<List<RowMetaAndData>>> consumerGrabber;

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
    when( dataService.getServiceTrans() ).thenReturn( transMeta );
//    when( transMeta.realClone( false ) ).thenReturn( transMeta );

    doAnswer( new Answer<Void>() {
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
    } ).when( annotationsQueryService ).prepareQuery( anyString(), anyInt(), anyMap() );

    when( dataServiceExecutor.getServiceTrans() ).thenReturn( mock( Trans.class ) );
    when( dataServiceExecutor.getGenTrans() ).thenReturn( mock( Trans.class ) );
    when( dataServiceExecutor.getServiceTrans().getLogChannel() ).thenReturn( mock( LogChannelInterface.class ) );
    when( dataServiceExecutor.getGenTrans().getLogChannel() ).thenReturn( mock( LogChannelInterface.class ) );
    when( dataServiceExecutor.getSql() ).thenReturn( mock( SQL.class ) );
    when( dataServiceExecutor.getSql().getSelectFields() ).thenReturn( mock( SQLFields.class ) );
    when( dataServiceExecutor.getSql().getRowMeta() ).thenReturn( new RowMeta() );
    when( dataServiceExecutor.getSql().getSelectFields().getFields() )
      .thenReturn( new ArrayList<SQLField>() );

    when( transMeta.listParameters() ).thenReturn( new String[] { "foo", "bar" } );
    when( transMeta.getParameterDefault( "foo" ) ).thenReturn( "fooVal" );
    when( transMeta.getParameterDefault( "bar" ) ).thenReturn( "barVal" );

    lenient().when( bindingFactory.createBinding( same( model ), anyString(), any( XulComponent.class ), anyString() ) ).thenReturn( binding );
    lenient().when( bindingFactory.createBinding( same( model ), anyString(), any( XulComponent.class ), anyString(), any() ) ).thenReturn( binding );

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
  public void testInitAssertsFail() throws Exception {
    when( document.getElementById( "log-levels" ) ).thenReturn( xulTextBox );
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

    testInitAssertFail();

    when( document.getElementById( "log-levels" ) ).thenReturn( xulMenuList );
    when( document.getElementById( "sql-textbox" ) ).thenReturn( xulMenuList );

    testInitAssertFail();

    when( document.getElementById( "sql-textbox" ) ).thenReturn( xulTextBox );
    when( document.getElementById( "maxrows-combo" ) ).thenReturn( xulTextBox );

    testInitAssertFail();

    when( document.getElementById( "maxrows-combo" ) ).thenReturn( xulMenuList );
    when( document.getElementById( "streaming-groupbox" ) ).thenReturn( xulMenuList );

    testInitAssertFail();

    when( document.getElementById( "streaming-groupbox" ) ).thenReturn( xulGroupbox );
    when( document.getElementById( "time-based-radio" ) ).thenReturn( xulGroupbox );

    testInitAssertFail();

    when( document.getElementById( "time-based-radio" ) ).thenReturn( xulRadio );
    when( document.getElementById( "row-based-radio" ) ).thenReturn( xulGroupbox );

    testInitAssertFail();

    when( document.getElementById( "row-based-radio" ) ).thenReturn( xulRadio );
    when( document.getElementById( "window-size" ) ).thenReturn( xulGroupbox );

    testInitAssertFail();

    when( document.getElementById( "window-size" ) ).thenReturn( xulTextBox );
    when( document.getElementById( "window-every" ) ).thenReturn( xulGroupbox );

    testInitAssertFail();

    when( document.getElementById( "window-every" ) ).thenReturn( xulTextBox );
    when( document.getElementById( "window-limit" ) ).thenReturn( xulGroupbox );

    testInitAssertFail();

    when( document.getElementById( "window-limit" ) ).thenReturn( xulTextBox );
    when( document.getElementById( "window-size-time-unit" ) ).thenReturn( xulTextBox );

    testInitAssertFail();

    when( document.getElementById( "window-size-time-unit" ) ).thenReturn( xulLabel );
    when( document.getElementById( "window-every-time-unit" ) ).thenReturn( xulTextBox );

    testInitAssertFail();

    when( document.getElementById( "window-every-time-unit" ) ).thenReturn( xulLabel );
    when( document.getElementById( "window-limit-time-unit" ) ).thenReturn( xulTextBox );

    testInitAssertFail();

    when( document.getElementById( "window-limit-time-unit" ) ).thenReturn( xulLabel );
    when( document.getElementById( "window-size-row-unit" ) ).thenReturn( xulTextBox );

    testInitAssertFail();

    when( document.getElementById( "window-size-row-unit" ) ).thenReturn( xulLabel );
    when( document.getElementById( "window-every-row-unit" ) ).thenReturn( xulTextBox );

    testInitAssertFail();

    when( document.getElementById( "window-every-row-unit" ) ).thenReturn( xulLabel );
    when( document.getElementById( "window-limit-row-unit" ) ).thenReturn( xulTextBox );

    testInitAssertFail();

    when( document.getElementById( "window-limit-row-unit" ) ).thenReturn( xulLabel );

    testInitAssertNotFail();
  }

  private void testInitAssertFail() throws Exception {
    try {
      dataServiceTestController.init();
      fail();
    } catch ( AssertionError e ) {
      // Pass test
    } catch ( Exception e ) {
      fail();
    }
  }

  private void testInitAssertNotFail() throws Exception {
    try {
      dataServiceTestController.init();
    } catch ( AssertionError | Exception e ) {
      fail();
    }
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
    when( document.getElementById( "preview-opt-btn" ) ).thenReturn( xulButton );
    when( document.getElementById( "exec-sql-btn" ) ).thenReturn( xulButton );
    when( document.getElementById( "error-alert" ) ).thenReturn( xulLabel );
    when( document.getElementById( "optimization-impact-info" ) ).thenReturn( xulTextBox );

    dataServiceTestController.init();

    verify( bindingFactory ).setDocument( document );
    verify( document, times( 34 ) ).getElementById( anyString() );
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
    inOrder.verify( dataServiceExecutor ).executeQuery( any( Observer.class ) );
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
    when( dataServiceExecutor.getGenTrans().getErrors() ).thenReturn( 1 );
    lenient().when( dataServiceExecutor.getGenTrans().isFinishedOrStopped() )
      .thenReturn( true );
    dataServiceTestController.executeSql();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass( String.class );

    verify( model, timeout( VERIFY_TIMEOUT_MILLIS ).atLeast( 2 ) ).setAlertMessage( argument.capture() );
    assertTrue( argument.getValue().length() > 0 );
    assertEquals( "There were errors, review logs.", argument.getValue() );
  }

  @Test
  public void errorAlertMessageSetIfErrorInSvcTrans() throws KettleException {
    when( dataServiceExecutor.getServiceTrans().getErrors() ).thenReturn( 1 );
    dataServiceTestController.executeSql();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass( String.class );
    verify( model, timeout( VERIFY_TIMEOUT_MILLIS ).atLeast( 2 ) ).setAlertMessage( argument.capture() );
    assertTrue( argument.getValue().length() > 0 );
    assertEquals( "There were errors, review logs.", argument.getValue() );
  }

  @Test
  public void hideStreamingTest() throws KettleException {
    when( dataService.isStreaming() ).thenReturn( false );
    assertTrue( dataServiceTestController.hideStreaming() );
    when( dataService.isStreaming() ).thenReturn( true );
    assertFalse( dataServiceTestController.hideStreaming() );
  }

  @Test
  public void callbackNotifiedOnExecutionComplete() throws Exception {
    dataServiceTestController.executeSql();
    lenient().when( dataServiceExecutor.getServiceTrans().isFinishedOrStopped() )
      .thenReturn( true );
    lenient().when( dataServiceExecutor.getGenTrans().isFinishedOrStopped() )
      .thenReturn( true );
    verify( callback, timeout( VERIFY_TIMEOUT_MILLIS ).times( 1 ) ).onExecuteComplete();
  }

  @Test
  public void callbackNotifiedOnExecutionCompleteStreaming() throws Exception {
    dataServiceTestController.executeSql();
    lenient().when( dataServiceExecutor.getGenTrans().isFinished() )
      .thenReturn( true );
    verify( callback, timeout( VERIFY_TIMEOUT_MILLIS ).times( 1 ) ).onExecuteComplete();
  }

  @Test
  public void testExecuteStreamingServiceTransRunning() throws Exception {
    when( dataService.isStreaming() ).thenReturn( true );
    when( dataServiceExecutor.getServiceTrans().isRunning() )
      .thenReturn( true );
    dataServiceTestController.executeSql();
    verify( model, times( 0 ) ).clearOptimizationImpact();
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
  public void streamingQueryForAnnotations() throws KettleException, IOException {
    when( model.getSql() ).thenReturn( MOCK_SQL );
    when( model.getWindowMode() ).thenReturn( MOCK_WINDOW_MODE );
    when( model.getWindowSize() ).thenReturn( MOCK_WINDOW_SIZE );
    when( model.getWindowEvery() ).thenReturn( MOCK_WINDOW_EVERY );
    when( model.getWindowLimit() ).thenReturn( MOCK_WINDOW_LIMIT );

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

  @Test
  public void testExecuteStreamingService() throws Exception {
    when( dataService.isStreaming() ).thenReturn( true );
    when( dataServiceExecutor.executeStreamingQuery( consumerGrabber.capture(), eq( false ) ) )
        .thenReturn( dataServiceExecutor );

    RowMetaInterface rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaInteger( "i" ) );

    dataServiceTestController.executeSql();
    Disposable disp = mock( Disposable.class );
    Observer<List<RowMetaAndData>> consumer = consumerGrabber.getValue();
    consumer.onSubscribe( disp );
    consumer.onNext( Arrays.asList( new RowMetaAndData( rowMeta, 1 ), new RowMetaAndData( rowMeta, 2 ) ) );
    consumer.onNext( Arrays.asList( new RowMetaAndData( rowMeta, 3 ) ) );
    dataServiceTestController.close();

    verify( model, times( 2 ) ).setResultRowMeta( eq( rowMeta ) );
    verify( model, times( 3 ) ).addResultRow( any() );
    verify( callback, times( 1 ) ).onExecuteComplete();
    verify( callback, times( 1 ) ).onUpdate( any() );
    verify( disp ).dispose();
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
