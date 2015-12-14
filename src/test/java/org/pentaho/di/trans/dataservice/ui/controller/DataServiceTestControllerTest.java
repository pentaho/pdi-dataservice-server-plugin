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

package org.pentaho.di.trans.dataservice.ui.controller;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
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
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.core.sql.SQLField;
import org.pentaho.di.core.sql.SQLFields;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.ui.DataServiceTestCallback;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceTestModel;
import org.pentaho.di.trans.step.RowListener;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.components.XulMenuList;
import org.pentaho.ui.xul.components.XulTextbox;
import org.pentaho.ui.xul.dom.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
  private DataServiceContext context;

  private DataServiceTestControllerTester dataServiceTestController;

  @Before
  public void initMocks() throws Exception {
    MockitoAnnotations.initMocks( this );

    when( dataService.getServiceTrans() ).thenReturn( transMeta );

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

    when( transMeta.listParameters() ).thenReturn( new String[] { "foo", "bar" } );
    when( transMeta.getParameterDefault( "foo" ) ).thenReturn( "fooVal" );
    when( transMeta.getParameterDefault( "bar" ) ).thenReturn( "barVal" );

    when( bindingFactory.createBinding( anyObject(), anyString(), anyObject(), anyString() ) ).thenReturn( binding );

        // mocks to deal with Xul multithreading.
        when( xulDomContainer.getDocumentRoot() ).thenReturn( document );
    doAnswer( new Answer() {
          @Override public Object answer( InvocationOnMock invocationOnMock ) throws Throwable {
            ( (Runnable) invocationOnMock.getArguments()[0] ).run();
            return null;
          }
        } ).when( document ).invokeLater( any( Runnable.class ) );

    dataServiceTestController = new DataServiceTestControllerTester();
    dataServiceTestController.setXulDomContainer( xulDomContainer );
  }

  @Test
  public void testInit() throws Exception {
    when( document.getElementById( "log-levels" ) ).thenReturn( xulMenuList );
    when( document.getElementById( "sql-textbox" ) ).thenReturn( xulTextBox );
    when( document.getElementById( "maxrows-combo" ) ).thenReturn( xulMenuList );

    dataServiceTestController.init();

    verify( bindingFactory ).setDocument( document );
    verify( document, times( 9 ) ).getElementById( anyString() );
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
    verify( model, timeout( 500 ).atLeast( 2 ) ).setAlertMessage( argument.capture() );
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
    verify( model, timeout( 500 ).atLeast( 2 ) ).setAlertMessage( argument.capture() );
    assertTrue( argument.getValue().length() > 0 );
    assertEquals( "There were errors, review logs.", argument.getValue() );
  }

  @Test
  public void callbackNotifiedOnExecutionComplete() throws Exception {
    when( model.isExecuting() ).thenReturn( true );
    dataServiceTestController.executeSql();
    when( dataServiceExecutor.getServiceTrans().isFinishedOrStopped() )
      .thenReturn( true );
    when( dataServiceExecutor.getGenTrans().isFinishedOrStopped() )
      .thenReturn( true );
    verify( callback, timeout( 500 ).times( 1 ) ).onExecuteComplete();
  }

  @Test
  public void resultRowMetaIsUpdated() throws Exception {
    SQLField field = mock( SQLField.class );
    List<SQLField> fields = new ArrayList<SQLField>();
    fields.add( field );
    when( field.getField() ).thenReturn( "testFieldName" );
    when( dataServiceExecutor.getSql().getSelectFields().getFields() )
      .thenReturn( fields );
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMeta( "testFieldName" ) );
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
    verify( dataServiceExecutor.getServiceTrans(), never() ).prepareExecution( any( String[].class ) );
    dataServiceTestController.close();
    ArgumentCaptor<NamedParams> paramCaptor = ArgumentCaptor.forClass( NamedParams.class );
    verify( transMeta ).copyParametersFrom( paramCaptor.capture() );

    NamedParams params = paramCaptor.getValue();
    assertThat( Arrays.asList( params.listParameters() ), containsInAnyOrder( "foo", "bar" ) );
    assertThat( params.getParameterDefault( "foo" ), is( "fooVal" ) );
    assertThat( params.getParameterDefault( "bar" ), is( "barVal" ) );
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
