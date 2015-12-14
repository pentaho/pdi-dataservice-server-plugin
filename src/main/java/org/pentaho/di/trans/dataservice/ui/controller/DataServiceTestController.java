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

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.logging.LoggingRegistry;
import org.pentaho.di.core.logging.MetricsRegistry;
import org.pentaho.di.core.parameters.NamedParams;
import org.pentaho.di.core.parameters.NamedParamsDefault;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.core.sql.SQLField;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.ui.DataServiceTestCallback;
import org.pentaho.di.trans.dataservice.ui.DataServiceTestDialog;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceTestModel;
import org.pentaho.di.trans.step.RowListener;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingConvertor;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.binding.DefaultBindingFactory;
import org.pentaho.ui.xul.components.XulButton;
import org.pentaho.ui.xul.components.XulLabel;
import org.pentaho.ui.xul.components.XulMenuList;
import org.pentaho.ui.xul.components.XulTextbox;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;

public class DataServiceTestController extends AbstractXulEventHandler {

  public static final int POLL_DELAY_MILLIS = 500;
  public static final int POLL_PERIOD_MILLIS = 500;
  public static Class<?> PKG = DataServiceTestDialog.class;

  private final DataServiceTestModel model;

  private static final String NAME = "dataServiceTestController";

  private final String transName;

  private DataServiceTestCallback callback;

  private final DataServiceMeta dataService;
  private final TransMeta transMeta;
  private final NamedParams startingParameterValues = new NamedParamsDefault();

  private XulMenuList<String> logLevels;
  private Timer completionPollTimer;
  private DataServiceExecutor dataServiceExec;
  private DataServiceContext context;

  private BindingFactory bindingFactory;

  private XulMenuList<String> maxRows;

  public DataServiceTestController( DataServiceTestModel model, DataServiceMeta dataService, DataServiceContext context ) throws KettleException {
    this( model, dataService, new DefaultBindingFactory(), context );
  }

  public DataServiceTestController( DataServiceTestModel model, DataServiceMeta dataService,
      BindingFactory bindingFactory, DataServiceContext context ) throws KettleException {
    this.model = model;
    this.dataService = dataService;
    this.transMeta = dataService.getServiceTrans();
    this.bindingFactory = bindingFactory;
    transName = transMeta.getName();
    this.context = context;
    model.setSql( getDefaultSql() );
    initStartingParameterValues();

    setName( NAME );
  }

  /**
   * Captures parameter values at initialization time to allow
   * reverting any changes made to the service TransMeta.
   */
  void initStartingParameterValues() {
    startingParameterValues.copyParametersFrom( transMeta );
  }

  public void init() throws InvocationTargetException, XulException {
    bindingFactory.setDocument( this.getXulDomContainer().getDocumentRoot() );

    bindLogLevelsCombo( bindingFactory );
    bindSqlText( bindingFactory );
    bindButtons( bindingFactory );
    bindMaxRowsCombo( bindingFactory );
    bindErrorAlert( bindingFactory );
    bindOptImpactInfo( bindingFactory );
  }

  private void bindButtons( BindingFactory bindingFactory ) throws InvocationTargetException, XulException {
    bindButton( bindingFactory, (XulButton) document.getElementById( "preview-opt-btn" ) );
    bindButton( bindingFactory, (XulButton) document.getElementById( "exec-sql-btn" ) );
  }

  /**
   * Binds the button to the executing prop of model, such that the button will be
   * disabled for the duration of a query execution.
   */
  private void bindButton( BindingFactory bindingFactory, XulButton button ) throws XulException, InvocationTargetException {
    bindingFactory.setBindingType( Binding.Type.ONE_WAY );
    Binding binding = bindingFactory.createBinding( model, "executing", button, "disabled" );
    binding.initialize();
    binding.fireSourceChanged();
  }

  private void bindErrorAlert( BindingFactory bindingFactory ) throws InvocationTargetException, XulException {
    XulLabel errorAlert = (XulLabel) document.getElementById( "error-alert" );
    model.setAlertMessage( "" );
    bindingFactory.setBindingType( Binding.Type.ONE_WAY );
    Binding binding = bindingFactory.createBinding( model, "alertMessage", errorAlert, "value" );
    binding.initialize();
    binding.fireSourceChanged();
  }

  private void bindOptImpactInfo( BindingFactory bindingFactory ) {
    XulTextbox maxRows = (XulTextbox) document.getElementById( "optimization-impact-info" );
    bindingFactory.setBindingType( Binding.Type.ONE_WAY );
    Binding binding = bindingFactory.createBinding( model, "optimizationImpactDescription", maxRows, "value" );
    binding.initialize();
  }

  private void bindMaxRowsCombo( BindingFactory bindingFactory ) throws InvocationTargetException, XulException {
    bindMaxRowsComboValues( bindingFactory );
    bindSelectedMaxRows( bindingFactory );
  }

  @SuppressWarnings( "unchecked" )
  private void bindMaxRowsComboValues( BindingFactory bindingFactory ) throws InvocationTargetException, XulException {
    assert document.getElementById( "maxrows-combo" ) instanceof XulMenuList;
    maxRows = (XulMenuList<String>) document.getElementById( "maxrows-combo" );
    bindingFactory.setBindingType( Binding.Type.ONE_WAY );
    bindingFactory.createBinding( model, "allMaxRows", maxRows, "elements" )
      .fireSourceChanged();
  }

  private void bindSelectedMaxRows( BindingFactory bindingFactory ) throws InvocationTargetException, XulException {
    Binding binding = bindingFactory.createBinding( model, "maxRows", maxRows, "selectedItem" );
    binding.setBindingType( Binding.Type.BI_DIRECTIONAL );
    BindingConvertor maxRowsConverter = new BindingConvertor<Integer, Integer>() {
      @Override
      public Integer sourceToTarget( Integer value ) {
        return DataServiceTestModel.MAXROWS_CHOICES.indexOf( value );
      }

      @Override
      public Integer targetToSource( Integer value ) {
        return DataServiceTestModel.MAXROWS_CHOICES.indexOf( value );
      }
    };
    binding.setConversion( maxRowsConverter );
    binding.initialize();
    binding.fireSourceChanged();
  }

  private void bindLogLevelsCombo( BindingFactory bindingFactory ) throws InvocationTargetException, XulException {
    bindLogLevelComboValues( bindingFactory );
    bindSelectedLogLevel( bindingFactory );
  }

  @SuppressWarnings( "unchecked" )
  private void bindLogLevelComboValues( BindingFactory bindingFactory ) throws XulException, InvocationTargetException {
    assert document.getElementById( "log-levels" ) instanceof XulMenuList;
    logLevels = (XulMenuList<String>) document.getElementById( "log-levels" );
    bindingFactory.setBindingType( Binding.Type.ONE_WAY );
    bindingFactory.createBinding( model, "allLogLevels", logLevels, "elements" )
      .fireSourceChanged();
  }

  private void bindSelectedLogLevel( BindingFactory bindingFactory ) throws InvocationTargetException, XulException {
    Binding logBinding = bindingFactory.createBinding( model, "logLevel", logLevels, "selectedItem" );
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
    String initSql = getDefaultSql();
    model.setSql( initSql );
    sqlTextBox.setValue( initSql );

    bindingFactory.setBindingType( Binding.Type.BI_DIRECTIONAL );
    bindingFactory.createBinding( model, "sql", sqlTextBox, "value" );
  }

  private String getDefaultSql() {
    return "SELECT * FROM " + dataService.getName();
  }

  public void executeSql() throws KettleException {
    resetMetrics();
    dataServiceExec = getNewDataServiceExecutor( true );

    updateOptimizationImpact( dataServiceExec );
    updateModel( dataServiceExec );
    callback.onLogChannelUpdate();
    dataServiceExec.executeQuery( getDataServiceRowListener() );
    pollForCompletion( dataServiceExec );
  }

  private void pollForCompletion( final DataServiceExecutor dataServiceExec ) {
    final Trans svcTrans = dataServiceExec.getServiceTrans();
    final Trans genTrans = dataServiceExec.getGenTrans();
    completionPollTimer = new Timer( "DataServiceTesterTimer" );
    final long startMillis = System.currentTimeMillis();

    TimerTask task = new TimerTask() {
      @Override
      public void run() {
        // any actions that might update widgets need to be invoked in UI thread
        document.invokeLater( new Runnable() {
          @Override
          public void run() {
            if ( model.isExecuting() ) {
              checkMaxRows( dataServiceExec );
              checkForFailures( dataServiceExec );
              updateExecutingMessage( startMillis, dataServiceExec );

              if ( anyTransErrors( dataServiceExec ) || transDone( svcTrans, genTrans ) ) {
                handleCompletion( dataServiceExec );
                completionPollTimer.cancel();
              }
            }
          }
        } );
      }
    };
    completionPollTimer.schedule( task, POLL_DELAY_MILLIS, POLL_PERIOD_MILLIS );
  }

  private boolean transDone( Trans svcTrans, Trans genTrans ) {
    return svcTrans.isFinishedOrStopped() && genTrans.isFinishedOrStopped();
  }

  private boolean anyTransErrors( DataServiceExecutor dataServiceExec ) {
    return dataServiceExec.getServiceTrans().getErrors() > 0
      || dataServiceExec.getGenTrans().getErrors() > 0;
  }

  private void checkForFailures( DataServiceExecutor dataServiceExec ) {
    if ( anyTransErrors( dataServiceExec ) ) {
      setErrorAlertMessage();
      stopDataService( dataServiceExec );
    }
  }

  private void checkMaxRows( DataServiceExecutor dataServiceExec ) {
    if ( model.getMaxRows() > 0
      && model.getMaxRows() <= model.getResultRows().size() ) {
      //Exceeded max rows, no need to continue
      stopDataService( dataServiceExec );
    }
  }

  private void stopDataService( DataServiceExecutor dataServiceExec ) {
    dataServiceExec.stop();
  }

  private void updateExecutingMessage( long start, DataServiceExecutor dataServiceExec ) {
    if ( !anyTransErrors( dataServiceExec ) ) {
      model.setAlertMessage(
        BaseMessages.getString( PKG, "DataServiceTest.RowsReturned.Text",
          model.getResultRows().size(),
          System.currentTimeMillis() - start ) );
    }
  }

  private void handleCompletion( DataServiceExecutor dataServiceExec ) {
    maybeSetErrorAlert( dataServiceExec );
    // revert the trans name to the original name.  DataServiceExecutor
    // changes it for logging purposes.
    transMeta.setName( transName );
    model.setExecuting( false );
    callback.onExecuteComplete();
  }

  private void resetMetrics() {
    resetMetrics( model.getServiceTransLogChannel() );
    resetMetrics( model.getGenTransLogChannel() );
  }

  private void resetMetrics( LogChannelInterface logChannel ) {
    LoggingRegistry loggingRegistry = LoggingRegistry.getInstance();
    MetricsRegistry metricsRegistry = MetricsRegistry.getInstance();
    if ( logChannel != null ) {
      for ( String channelId : loggingRegistry.getLogChannelChildren( logChannel.getLogChannelId() ) ) {
        metricsRegistry.getSnapshotLists().remove( channelId );
      }
    }
  }

  @SuppressWarnings( "unused" ) // Bound via XUL
  public void previewQueries() throws KettleException {
    DataServiceExecutor dataServiceExec = getNewDataServiceExecutor( false );
    updateOptimizationImpact( dataServiceExec );
  }

  private void updateOptimizationImpact( DataServiceExecutor dataServiceExec ) {
    model.clearOptimizationImpact();

    for ( PushDownOptimizationMeta optMeta : dataService.getPushDownOptimizationMeta() ) {
      model.addOptimizationImpact( optMeta.preview( dataServiceExec ) );
    }
  }

  private void maybeSetErrorAlert( DataServiceExecutor dataServiceExec ) {
    if ( dataServiceExec.getGenTrans().getErrors() > 0
      || ( dataServiceExec.getServiceTrans() != null
      && dataServiceExec.getServiceTrans().getErrors() > 0 ) ) {
      setErrorAlertMessage();
    }
  }

  private void setErrorAlertMessage() {
    model.setAlertMessage(
      BaseMessages.getString( PKG, "DataServiceTest.Errors.Label" ) );
  }

  protected DataServiceExecutor getNewDataServiceExecutor( boolean enableMetrics ) throws KettleException {
    try {
      resetVariablesAndParameters();
      return new DataServiceExecutor.Builder( new SQL( model.getSql() ), dataService, context ).
        rowLimit( model.getMaxRows() ).
        logLevel( model.getLogLevel() ).
        enableMetrics( enableMetrics ).
        build();
    } catch ( KettleException e ) {
      model.setAlertMessage( e.getMessage() );
      throw e;
    }
  }

  /**
   * Assures variables are consistent between DatabaseMeta and transMeta.
   * Otherwise DatabaseMeta may have parameter values saved from previous execution.
   * Also reverts Parameter values to their initial settings when the Controller
   * was constructed.
   */
  private void resetVariablesAndParameters() throws KettleException {
    for ( StepMeta stepMeta : transMeta.getSteps() ) {
      if ( stepMeta.getStepMetaInterface() instanceof TableInputMeta ) {
        DatabaseMeta dbMeta = ( (TableInputMeta) stepMeta.getStepMetaInterface() ).getDatabaseMeta();
        if ( dbMeta != null ) {
          dbMeta.copyVariablesFrom( transMeta );
        }
      }
    }
    if ( startingParameterValues.listParameters().length > 0 ) {
      transMeta.copyParametersFrom( startingParameterValues );
    }
  }

  private void updateModel( DataServiceExecutor dataServiceExec ) throws KettleException {
    model.setExecuting( true );
    model.setResultRowMeta( sqlFieldsToRowMeta( dataServiceExec ) );
    model.clearResultRows();
    model.setAlertMessage( "" );

    model.setServiceTransLogChannel( dataServiceExec.getServiceTrans().getLogChannel() );
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

  public void close() throws KettleException {
    cleanupCurrentExec();
    resetVariablesAndParameters();
    clearLogLines();
    callback.onClose();
  }

  private void cleanupCurrentExec() {
    if ( completionPollTimer != null ) {
      completionPollTimer.cancel();
    }
    if ( dataServiceExec != null ) {
      stopDataService( dataServiceExec );
    }
    model.setExecuting( false );
  }

  private void clearLogLines() {
    if ( model.getGenTransLogChannel() != null ) {
      KettleLogStore.discardLines( model.getGenTransLogChannel().getLogChannelId(), true );
    }
    if ( model.getServiceTransLogChannel() != null ) {
      KettleLogStore.discardLines( model.getServiceTransLogChannel().getLogChannelId(), true );
    }
  }

  public void setCallback( DataServiceTestCallback callback ) {
    this.callback = callback;
  }
}
