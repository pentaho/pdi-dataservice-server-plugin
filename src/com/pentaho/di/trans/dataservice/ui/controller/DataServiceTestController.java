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
import com.pentaho.di.trans.dataservice.ui.DataServiceTestDialog;
import com.pentaho.di.trans.dataservice.ui.model.DataServiceTestModel;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.sql.SQLField;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.RowListener;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingConvertor;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.binding.DefaultBinding;
import org.pentaho.ui.xul.binding.DefaultBindingFactory;
import org.pentaho.ui.xul.components.XulLabel;
import org.pentaho.ui.xul.components.XulMenuList;
import org.pentaho.ui.xul.components.XulTextbox;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class DataServiceTestController extends AbstractXulEventHandler {

  private static Class<?> PKG = DataServiceTestDialog.class;

  private final DataServiceTestModel model;

  private static final String NAME = "dataServiceTestController";

  private final String transName;

  private DataServiceTestCallback callback;

  private final DataServiceMeta dataService;
  private final TransMeta transMeta;
  private XulMenuList<String> logLevels;

  public DataServiceTestController( DataServiceTestModel model,
                                    DataServiceMeta dataService,
                                    TransMeta transMeta ) {
    this.model = model;
    this.dataService = dataService;
    this.transMeta = transMeta;
    transName = transMeta.getName();
    setName( NAME );
  }

  public void init() throws InvocationTargetException, XulException {
    BindingFactory bindingFactory = new DefaultBindingFactory();
    bindingFactory.setDocument( this.getXulDomContainer().getDocumentRoot() );

    bindLogLevelsCombo( bindingFactory );
    bindSqlText( bindingFactory );
    bindMaxRows( bindingFactory );
    bindErrorAlert( bindingFactory );
  }

  private void bindErrorAlert( BindingFactory bindingFactory ) throws InvocationTargetException, XulException {
    XulLabel errorAlert = (XulLabel) document.getElementById( "error-alert" );
    model.setErrorAlertMessage( "" );
    bindingFactory.setBindingType( Binding.Type.ONE_WAY );
    Binding binding = bindingFactory.createBinding( model, "errorAlertMessage", errorAlert, "value" );
    binding.initialize();
    binding.fireSourceChanged();
  }

  private void bindMaxRows( BindingFactory bindingFactory ) throws InvocationTargetException, XulException {
    XulTextbox maxRows = (XulTextbox) document.getElementById( "maxrows-textbox" );
    bindingFactory.setBindingType( Binding.Type.BI_DIRECTIONAL );
    Binding binding = bindingFactory.createBinding( model, "maxRows", maxRows, "value" );
    BindingConvertor maxRowsConverter = new BindingConvertor<Integer, String>() {
      @Override
      public String sourceToTarget( Integer value ) {
        return value.toString();
      }

      @Override
      public Integer targetToSource( String value ) {
        return Integer.parseInt( value );
      }
    };
    binding.setConversion( maxRowsConverter );
    binding.initialize();
    binding.fireSourceChanged();
  }

  private void bindLogLevelsCombo( BindingFactory bindingFactory ) throws InvocationTargetException, XulException {
    bindLogLevelComboValues( bindingFactory );
    bindSelectedLogLevel();
  }

  private void bindLogLevelComboValues( BindingFactory bindingFactory ) throws XulException, InvocationTargetException {
    assert document.getElementById( "log-levels" ) instanceof XulMenuList;
    logLevels = (XulMenuList<String>) document.getElementById( "log-levels" );
    bindingFactory.setBindingType( Binding.Type.ONE_WAY );
    bindingFactory.createBinding( model, "allLogLevels", logLevels, "elements" )
      .fireSourceChanged();
  }

  private void bindSelectedLogLevel() throws InvocationTargetException, XulException {
    Binding logBinding = new DefaultBinding( model, "logLevel", logLevels, "selectedItem" );
    logBinding.setBindingType( Binding.Type.BI_DIRECTIONAL );
    BindingConvertor logLevelConverter = new BindingConvertor<LogLevel, String>() {
      @Override
      public String sourceToTarget( LogLevel value ) {
        return value.getDescription();
      }

      @Override
      public LogLevel targetToSource( String value ) {
        for ( LogLevel level : LogLevel.values() ) {
          if ( level.getDescription().equals( value ) ) {
            return level;
          }
        }
        throw new IllegalArgumentException(
          String.format( "'%s' does not correspond to a valid LogLevel value.", value ) );
      }
    };
    logBinding.setConversion( logLevelConverter );
    logBinding.initialize();
    logBinding.fireSourceChanged();
  }

  private void bindSqlText( BindingFactory bindingFactory ) {
    XulTextbox sqlTextBox = (XulTextbox) document.getElementById( "sql-textbox" );
    String initSql = "SELECT * FROM " + dataService.getName();
    model.setSql( initSql );
    sqlTextBox.setValue( initSql );

    bindingFactory.setBindingType( Binding.Type.BI_DIRECTIONAL );
    bindingFactory.createBinding( model, "sql", sqlTextBox, "value" );
  }

  public void executeSql() throws KettleException {
    DataServiceExecutor dataServiceExec = getNewDataServiceExecutor();

    updateModel( dataServiceExec );
    callback.onLogChannelUpdate();
    try {
      dataServiceExec.executeQuery( getDataServiceRowListener() );
      dataServiceExec.waitUntilFinished();
      maybeSetErrorAlert( dataServiceExec );
    } catch ( KettleException ke ) {
      setErrorAlertMessage();
    }
    // revert the trans name to the original name.  DataServiceExecutor
    // changes it for logging purposes.
    transMeta.setName( transName );

    callback.onExecuteComplete();
  }

  private void maybeSetErrorAlert( DataServiceExecutor dataServiceExec ) {
    if ( dataServiceExec.getGenTrans().getErrors() > 0
      || ( dataServiceExec.getServiceTrans() != null
      && dataServiceExec.getServiceTrans().getErrors() > 0 ) ) {
      setErrorAlertMessage();
    }
  }

  private void setErrorAlertMessage() {
    model.setErrorAlertMessage(
      BaseMessages.getString( PKG, "DataServiceTest.Errors.Label" ) );
  }

  protected DataServiceExecutor getNewDataServiceExecutor() throws KettleException {
    try {
      return new DataServiceExecutor( model.getSql(),
        Arrays.asList( dataService ), new HashMap<String, String>(),
        transMeta, model.getMaxRows() );
    } catch ( KettleException e ) {
      model.setErrorAlertMessage( e.getMessage() );
      throw e;
    }
  }

  private void updateModel( DataServiceExecutor dataServiceExec ) throws KettleException {
    model.setResultRowMeta( sqlFieldsToRowMeta( dataServiceExec ) );
    model.clearResultRows();
    model.setErrorAlertMessage( "" );

    dataServiceExec.getGenTrans().setLogLevel( model.getLogLevel() );
    dataServiceExec.getServiceTrans().setLogLevel( model.getLogLevel() );
    model.setServiceTransLogChannel(
      dataServiceExec.isDual() ? null : dataServiceExec.getServiceTrans().getLogChannel() );
    model.setGenTransLogChannel( dataServiceExec.getGenTrans().getLogChannel() );
  }

  private RowMetaInterface sqlFieldsToRowMeta( DataServiceExecutor dataServiceExec ) throws KettleException {
    List<SQLField> fields = dataServiceExec.getSql().getSelectFields().getFields();
    RowMetaInterface rowMeta = dataServiceExec.getSql().getRowMeta();
    RowMetaInterface sqlFieldsRowMeta = new RowMeta();
    List<String> fieldNames = Arrays.asList( rowMeta.getFieldNames() );
    for ( SQLField field : fields ) {
      int indexOfField = fieldNames.indexOf( field.getField() );
      sqlFieldsRowMeta.addValueMeta( rowMeta.getValueMeta( indexOfField ) );
    }
    return sqlFieldsRowMeta;
  }

  private RowListener getDataServiceRowListener() {
    return new RowListener() {
      @Override
      public void rowReadEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
        model.addResultRow( row );
        // if there were any rows read we want to make sure RowMeta is consistent w/ the
        // actual results, since datatypes may have changed (e.g. with sum(integer_field) )
        model.setResultRowMeta( rowMeta );
      }

      @Override
      public void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
        // throw away
      }

      @Override
      public void errorRowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
        model.addResultRow( row );
      }
    };
  }

  public void close() {
    KettleLogStore.discardLines( model.getGenTransLogChannel().getLogChannelId(), true );
    KettleLogStore.discardLines( model.getServiceTransLogChannel().getLogChannelId(), true );
    callback.onClose();
  }

  public void setCallback( DataServiceTestCallback callback ) {
    this.callback = callback;
  }

}
