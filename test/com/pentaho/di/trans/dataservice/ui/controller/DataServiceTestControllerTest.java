/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2014 Pentaho Corporation (Pentaho). All rights reserved.
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
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.RowListener;

import static junit.framework.Assert.assertTrue;
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

  @InjectMocks
  private DataServiceTestControllerTester dataServiceTestController;

  @Before
  public void initMocks() {
    MockitoAnnotations.initMocks( this );
    when( dataServiceExecutor.getServiceTrans() ).thenReturn( mock( Trans.class ) );
    when( dataServiceExecutor.getGenTrans() ).thenReturn( mock( Trans.class ) );
    when( dataServiceExecutor.getServiceTrans().getLogChannel() ).thenReturn( mock( LogChannelInterface.class ) );
    when( dataServiceExecutor.getGenTrans().getLogChannel() ).thenReturn( mock( LogChannelInterface.class ) );
  }

  @Test
  public void logChannelsAreUpdatedInModelBeforeSqlExec() throws KettleException {
    dataServiceTestController.executeSql();

    InOrder inOrder = inOrder( model, dataServiceExecutor, callback );
    inOrder.verify( model ).setServiceTransLogChannel(
      dataServiceExecutor.getServiceTrans().getLogChannel()  );
    inOrder.verify( model ).setGenTransLogChannel(
      dataServiceExecutor.getGenTrans().getLogChannel() );
    inOrder.verify( callback ).onLogChannelUpdate();
    inOrder.verify( dataServiceExecutor ).executeQuery( any( RowListener.class ) );
  }

  @Test
  public void errorAlertMessageBlankWhenNoError() throws KettleException {
    dataServiceTestController.executeSql();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass( String.class );
    // error message should be set to blank
    verify( model, times( 1 ) ).setErrorAlertMessage( argument.capture() );
    assertTrue( argument.getValue().equals( "" ) );
  }

  @Test
  public void errorAlertMessageSetIfErrorInGenTrans() throws KettleException {
    when( dataServiceExecutor.getGenTrans().getErrors() ).thenReturn( 1 );
    dataServiceTestController.executeSql();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass( String.class );
    verify( model, times( 2 ) ).setErrorAlertMessage( argument.capture() );
    assertTrue( argument.getValue().length() > 0 );
  }

  @Test
  public void errorAlertMessageSetIfErrorInSvcTrans() throws KettleException {
    when( dataServiceExecutor.getServiceTrans().getErrors() ).thenReturn( 1 );
    dataServiceTestController.executeSql();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass( String.class );
    verify( model, times( 2 ) ).setErrorAlertMessage( argument.capture() );
    assertTrue( argument.getValue().length() > 0 );
  }

  @Test
  public void callbackNotifiedOnExecutionComplete() throws Exception {
    dataServiceTestController.executeSql();

    InOrder inOrder = inOrder( dataServiceExecutor, callback );
    inOrder.verify( dataServiceExecutor ).waitUntilFinished();
    inOrder.verify( callback ).onExecuteComplete();
  }

  @Test
  public void previousResultsAreClearedOnSqlExec() throws KettleException {
    dataServiceTestController.executeSql();
    verify( model, times( 1 ) ).clearResultRows();
  }

  /**
   * Test class for purposes of injecting a mock DataServiceExecutor
   */
  static class DataServiceTestControllerTester extends DataServiceTestController {

    private DataServiceExecutor dataServiceExecutor;

    public DataServiceTestControllerTester( DataServiceTestModel model,
                                            DataServiceMeta dataService,
                                            TransMeta transMeta ) {
      super( model, dataService, transMeta );
    }

    public DataServiceTestControllerTester( DataServiceTestModel model,
                                            DataServiceMeta dataService,
                                            TransMeta transMeta,
                                            DataServiceExecutor dataServiceExecutor,
                                            DataServiceTestCallback callback ) {
      super( model, dataService, transMeta );
      this.dataServiceExecutor = dataServiceExecutor;
      setCallback( callback );
    }

    @Override
    protected DataServiceExecutor getDataServiceExecutor() throws KettleException {
      return dataServiceExecutor;
    }
  }
}
