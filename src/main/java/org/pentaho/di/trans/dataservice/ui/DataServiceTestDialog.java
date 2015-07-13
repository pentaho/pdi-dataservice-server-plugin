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

package org.pentaho.di.trans.dataservice.ui;

import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.ui.controller.DataServiceTestController;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceTestModel;
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

  private static final String XUL_PATH = "org/pentaho/di/trans/dataservice/ui/xul/dataservice-test-dialog.xul";
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
