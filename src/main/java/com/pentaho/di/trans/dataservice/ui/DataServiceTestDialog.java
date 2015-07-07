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

package com.pentaho.di.trans.dataservice.ui;

import com.pentaho.di.trans.dataservice.DataServiceMeta;
import com.pentaho.di.trans.dataservice.ui.controller.DataServiceTestController;
import com.pentaho.di.trans.dataservice.ui.model.DataServiceTestModel;
import org.eclipse.swt.widgets.Composite;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.metrics.MetricsDuration;
import org.pentaho.di.core.metrics.MetricsUtil;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.XulRunner;
import org.pentaho.ui.xul.containers.XulDialog;
import org.pentaho.ui.xul.dom.Document;
import org.pentaho.ui.xul.swt.SwtXulLoader;
import org.pentaho.ui.xul.swt.SwtXulRunner;

import java.util.Enumeration;
import java.util.List;
import java.util.ResourceBundle;

public class DataServiceTestDialog implements  java.io.Serializable {
  public static Class<?> PKG = DataServiceTestDialog.class;

  private static final String XUL_PATH = "com/pentaho/di/trans/dataservice/ui/xul/dataservice-test-dialog.xul";
  private static final String XUL_DIALOG_ID = "dataservice-test-dialog";
  private static final String GENTRANS_LOG_XUL_ID = "genTrans-log-tab";
  private static final String GENTRANS_METRICS_XUL_ID = "genTrans-metrics-tab";
  private static final String SVCTRANS_LOG_XUL_ID = "serviceTrans-log-tab";
  private static final String SVCTRANS_METRICS_XUL_ID = "serviceTrans-metrics-tab";
  private static final String QUERY_RESULTS_XUL_ID = "results-tab";

  private final DataServiceTestModel model = new DataServiceTestModel();
  private final DataServiceTestController dataServiceTestController;
  private final DataServiceTestResults resultsView;
  private final DataServiceTestLogBrowser serviceTransLogBrowser;
  private final DataServiceTestLogBrowser genTransLogBrowser;
  private final DataServiceTestMetrics serviceTransMetrics;
  private final DataServiceTestMetrics genTransMetrics;
  private final Document xulDocument;
  private final XulDialog dialog;

  private static final Class<?> CLZ = DataServiceTestDialog.class;
  private final ResourceBundle resourceBundle = new ResourceBundle() {
    @Override
    public Enumeration<String> getKeys() {
      return null;
    }

    @Override
    protected Object handleGetObject( String key ) {
      return BaseMessages.getString( CLZ, key );
    }
  };


  public DataServiceTestDialog( Composite parent, DataServiceMeta dataService,
                                TransMeta transMeta ) throws KettleException {
    try {
      dataServiceTestController = new DataServiceTestController( model, dataService, transMeta );
    } catch ( KettleException ke ) {
      new ErrorDialog( parent.getShell(), BaseMessages.getString( PKG, "DataServiceTest.TestDataServiceError.Title" ),
          BaseMessages.getString( PKG, "DataServiceTest.TestDataServiceError.Message" ), ke );
      throw ke;
    }
    xulDocument = initXul( parent );
    resultsView = initDataServiceResultsView( dataService, transMeta );
    serviceTransLogBrowser = new DataServiceTestLogBrowser( getComposite( SVCTRANS_LOG_XUL_ID ) );
    serviceTransMetrics = new DataServiceTestMetrics( getComposite( SVCTRANS_METRICS_XUL_ID ) );
    genTransLogBrowser = new DataServiceTestLogBrowser( getComposite( GENTRANS_LOG_XUL_ID ) );
    genTransMetrics = new DataServiceTestMetrics( getComposite( GENTRANS_METRICS_XUL_ID ) );
    dialog = (XulDialog) xulDocument.getElementById( XUL_DIALOG_ID );
    attachCallback();
  }

  private Composite getComposite( String elementId ) {
    return (Composite) xulDocument.getElementById( elementId ).getManagedObject();
  }

  public void open( ) throws KettleException {
    dialog.show();
  }

  public void close() {
    genTransLogBrowser.dispose();
    genTransMetrics.dispose();
    serviceTransLogBrowser.dispose();
    serviceTransMetrics.dispose();
    dialog.hide();
  }

  private DataServiceTestResults initDataServiceResultsView( DataServiceMeta dataService,
                                                             TransMeta transMeta ) throws KettleStepException {
    return new DataServiceTestResults( dataService, transMeta,
      getComposite( QUERY_RESULTS_XUL_ID ) );
  }

  private void attachCallback() {
    dataServiceTestController.setCallback(
        new DataServiceTestCallback() {
        @Override
        public void onExecuteComplete() {
          resultsView.setRowMeta( model.getResultRowMeta() );
          resultsView.load( model.getResultRows() );
          List<MetricsDuration> genTransMetrics = MetricsUtil.getAllDurations( model.getGenTransLogChannel().getLogChannelId() );
          DataServiceTestDialog.this.genTransMetrics.display( genTransMetrics );
          LogChannelInterface serviceTransLogChannel = model.getServiceTransLogChannel();
          if ( serviceTransLogChannel != null ) {
            serviceTransMetrics.display( MetricsUtil.getAllDurations( serviceTransLogChannel.getLogChannelId() ) );
          }
        }

        @Override
        public void onLogChannelUpdate() {
          updateLogChannel();
        }

        @Override
        public void onClose() {
          close();
        }
      }
    );
  }

  private void updateLogChannel() {
    if ( model.getServiceTransLogChannel() != null ) {
      serviceTransLogBrowser.attachToLogBrowser( model.getServiceTransLogChannel() );
    }
    if ( model.getGenTransLogChannel() != null ) {
      genTransLogBrowser.attachToLogBrowser( model.getGenTransLogChannel() );
    }
  }

  private Document initXul( Composite parent ) throws KettleException {
    try {
      SwtXulLoader swtLoader = new SwtXulLoader();
      swtLoader.setOuterContext( parent );
      swtLoader.registerClassLoader( getClass().getClassLoader() );
      XulDomContainer container = swtLoader.loadXul( XUL_PATH, resourceBundle );
      container.addEventHandler( dataServiceTestController );

      final XulRunner runner = new SwtXulRunner();
      runner.addContainer( container );
      runner.initialize();
      return container.getDocumentRoot();
    } catch ( XulException xulException ) {
      throw new KettleException( "Failed to initialize DataServicesTestDialog.",
        xulException );
    }
  }
}
