/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2015 Pentaho Corporation (Pentaho). All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Pentaho and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Pentaho and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Pentaho is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Pentaho,
 * explicitly covering such access.
 */

package com.pentaho.di.trans.dataservice.ui.controller;

import com.pentaho.di.trans.dataservice.DataServiceExecutor;
import com.pentaho.di.trans.dataservice.DataServiceMeta;
import com.pentaho.di.trans.dataservice.ui.DataServiceTestCallback;
import com.pentaho.di.trans.dataservice.ui.model.DataServiceTestModel;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.parameters.NamedParams;
import org.pentaho.di.core.parameters.UnknownParamException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.core.sql.SQLField;
import org.pentaho.di.core.sql.SQLFields;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.RowListener;
import org.pentaho.ui.xul.XulDomContainer;
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
import static org.mockito.Mockito.*;

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


  @InjectMocks
  private DataServiceTestControllerTester dataServiceTestController;

  @Before
  public void initMocks() throws UnknownParamException {
    MockitoAnnotations.initMocks( this );
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
    // mocks to deal with Xul multithreading.
    when( xulDomContainer.getDocumentRoot() ).thenReturn( document );
    dataServiceTestController.setXulDomContainer( xulDomContainer );
    doAnswer( new Answer() {
          @Override public Object answer( InvocationOnMock invocationOnMock ) throws Throwable {
            ( (Runnable) invocationOnMock.getArguments()[0] ).run();
            return null;
          }
        } ).when( document ).invokeLater( any( Runnable.class ) );
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
  static class DataServiceTestControllerTester extends DataServiceTestController {

    private DataServiceExecutor dataServiceExecutor;

    public DataServiceTestControllerTester( DataServiceTestModel model,
                                            DataServiceMeta dataService,
                                            TransMeta transMeta ) throws KettleException {
      super( model, dataService, transMeta );
    }

    public DataServiceTestControllerTester( DataServiceTestModel model,
                                            DataServiceMeta dataService,
                                            TransMeta transMeta,
                                            DataServiceExecutor dataServiceExecutor,
                                            DataServiceTestCallback callback ) throws KettleException {
      super( model, dataService, transMeta );
      this.dataServiceExecutor = dataServiceExecutor;
      setCallback( callback );
    }

    @Override
    protected DataServiceExecutor getNewDataServiceExecutor( boolean enableMetrics ) throws KettleException {
      return dataServiceExecutor;
    }
  }
}
