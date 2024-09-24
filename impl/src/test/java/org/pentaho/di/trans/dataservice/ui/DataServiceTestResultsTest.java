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
package org.pentaho.di.trans.dataservice.ui;

import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.KettleClientEnvironment;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.BaseTest;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.ui.controller.DataServiceTestController;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceTestModel;
import org.pentaho.di.trans.steps.delay.DelayMeta;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.DefaultBindingFactory;
import org.pentaho.ui.xul.impl.XulWindowContainer;
import org.pentaho.ui.xul.mock.DocumentAdapter;

import java.util.List;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;

public class DataServiceTestResultsTest extends BaseTest {

  @BeforeClass
  public static void setUp() throws Exception {
    KettleClientEnvironment.init();
    PluginRegistry.addPluginType( StepPluginType.getInstance() );
    StepPluginType.getInstance().handlePluginAnnotation(
      DelayMeta.class,
      DelayMeta.class.getAnnotation( org.pentaho.di.core.annotations.Step.class ),
      emptyList(), false, null );
    PluginRegistry.init();
    if ( !Props.isInitialized() ) {
      Props.init( 0 );
    }
  }

  @Test
  public void testGetCellTextFromObjWithoutLazyConversion() throws Exception {
    TransMeta transMeta = new TransMeta( getClass().getResource( "/DateToStr-without-lazy-conversion.ktr" ).getPath() );
    Trans trans = new Trans( transMeta );
    DataServiceMeta dataServiceMeta = new DataServiceMeta( transMeta );
    dataServiceMeta.setName( "Test" );
    dataServiceMeta.setStepname( "CSV file input" );
    DataServiceTestModel dataServiceTestModel = new DataServiceTestModel();
    DataServiceTestResults dataServiceTestResults = new DataServiceTestResults( dataServiceMeta, null );
    TestController dataServiceTestController = new TestController( dataServiceTestModel, dataServiceMeta, context );
    dataServiceTestController.setXulDomContainer( new XulWindowContainer() );
    attachCallback( dataServiceTestController, dataServiceTestModel, dataServiceTestResults );
    dataServiceTestModel.setSql( "SELECT * FROM \"Test\" WHERE DATE_TO_STR(ORDERDATE, 'yyyy-MM-dd') >= '2004-11-04'" );
    dataServiceTestController.executeSql();

    List<Object[]> resultRows = dataServiceTestModel.getResultRows();
    while ( resultRows.size() < 100 ) {
      resultRows = dataServiceTestModel.getResultRows();
    }

    RowMetaInterface rowMeta = dataServiceTestModel.getResultRowMeta();
    for ( Object[] rowData : resultRows ) {
      for ( int colNr = 0; colNr < rowMeta.size(); colNr++ ) {
        Object object = rowData[ colNr ];
        ValueMetaInterface valueMeta = rowMeta.getValueMeta( colNr );
        String cellText = dataServiceTestResults.getCellTextFromObj( object, valueMeta );
        String value = getValue( valueMeta, object );
        assertEquals( value, cellText );
      }
    }
    trans.stopAll();
  }

  @Test
  public void testGetCellTextFromObjWithLazyConversion() throws Exception {
    TransMeta transMeta = new TransMeta( getClass().getResource( "/DateToStr-with-lazy-conversion.ktr" ).getPath() );
    Trans trans = new Trans( transMeta );
    DataServiceMeta dataServiceMeta = new DataServiceMeta( transMeta );
    dataServiceMeta.setName( "Test" );
    dataServiceMeta.setStepname( "CSV file input" );
    DataServiceTestModel dataServiceTestModel = new DataServiceTestModel();
    DataServiceTestResults dataServiceTestResults = new DataServiceTestResults( dataServiceMeta, null );
    TestController dataServiceTestController = new TestController( dataServiceTestModel, dataServiceMeta, context );
    dataServiceTestController.setXulDomContainer( new XulWindowContainer() );
    attachCallback( dataServiceTestController, dataServiceTestModel, dataServiceTestResults );
    dataServiceTestModel.setSql( "SELECT * FROM \"Test\" WHERE DATE_TO_STR(ORDERDATE, 'yyyy-MM-dd') >= '2004-11-04'" );
    dataServiceTestController.executeSql();

    List<Object[]> resultRows = dataServiceTestModel.getResultRows();
    while ( resultRows.size() < 100 ) {
      resultRows = dataServiceTestModel.getResultRows();
    }

    RowMetaInterface rowMeta = dataServiceTestModel.getResultRowMeta();
    for ( Object[] rowData : resultRows ) {
      for ( int colNr = 0; colNr < rowMeta.size(); colNr++ ) {
        Object object = rowData[ colNr ];
        ValueMetaInterface valueMeta = rowMeta.getValueMeta( colNr );
        String cellText = dataServiceTestResults.getCellTextFromObj( object, valueMeta );
        String value = getValue( valueMeta, object );
        assertEquals( value, cellText );
      }
    }
    trans.stopAll();
  }

  private String getValue( ValueMetaInterface valueMeta, Object object ) throws Exception {
    String value = "";
    if ( valueMeta.isInteger() ) {
      value = valueMeta.getInteger( object ).toString();
    }
    if ( valueMeta.isDate() ) {
      value = valueMeta.getString( object );
    }
    if ( valueMeta.isBoolean() ) {
      value = valueMeta.getBoolean( object ).toString();
    }
    if ( valueMeta.isString() ) {
      value = valueMeta.getString( object );
    }
    if ( valueMeta.isBigNumber() ) {
      value = valueMeta.getBigNumber( object ).toString();
    }
    if ( valueMeta.isNumber() ) {
      value = valueMeta.getString( object );
    }
    if ( valueMeta.isBinary() ) {
      value = valueMeta.getBinary( object ).toString();
    }
    return value;
  }

  private class TestController extends DataServiceTestController {
    public TestController( DataServiceTestModel model, DataServiceMeta dataService, DataServiceContext context ) throws
      KettleException {
      super( model, dataService, new DefaultBindingFactory(), context );
    }

    public void setXulDomContainer( XulDomContainer xulDomContainer ) {
      this.xulDomContainer = xulDomContainer;
      this.document = new DocumentAdapter() {
        public void addInitializedBinding( Binding binding ) {
        }

        public void loadPerspective( String s ) {
        }
      };
    }
  }

  private void attachCallback( DataServiceTestController dataServiceTestController, DataServiceTestModel model,
                               DataServiceTestResults resultsView ) {
    dataServiceTestController.setCallback(
      new DataServiceTestCallback() {
        public void onExecuteComplete() {
        }

        public void onLogChannelUpdate() {
        }

        public void onClose() {
        }

        public void onUpdate( List<Object[]> rows ) {
        }
      }
    );
  }
}