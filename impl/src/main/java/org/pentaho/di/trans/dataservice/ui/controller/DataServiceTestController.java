/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.di.trans.dataservice.ui.controller;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import io.reactivex.Observer;
import io.reactivex.disposables.Disposable;
import io.reactivex.subjects.PublishSubject;

import org.apache.commons.io.IOUtils;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.logging.LoggingRegistry;
import org.pentaho.di.core.logging.MetricsRegistry;
import org.pentaho.di.core.parameters.NamedParams;
import org.pentaho.di.core.parameters.NamedParamsDefault;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.core.sql.SQLField;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;
import org.pentaho.di.trans.dataservice.clients.AnnotationsQueryService;
import org.pentaho.di.trans.dataservice.clients.Query;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.resolvers.DataServiceResolver;
import org.pentaho.di.trans.dataservice.ui.BindingConverters;
import org.pentaho.di.trans.dataservice.ui.DataServiceTestCallback;
import org.pentaho.di.trans.dataservice.ui.DataServiceTestDialog;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceTestModel;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.locator.api.MetastoreLocator;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingConvertor;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.binding.DefaultBindingFactory;
import org.pentaho.ui.xul.components.XulButton;
import org.pentaho.ui.xul.components.XulLabel;
import org.pentaho.ui.xul.components.XulMenuList;
import org.pentaho.ui.xul.components.XulRadio;
import org.pentaho.ui.xul.components.XulTextbox;
import org.pentaho.ui.xul.containers.XulGroupbox;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.stream.Collectors;


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
  private boolean stopQuery = false;
  private DataServiceContext context;

  private BindingFactory bindingFactory;

  private XulMenuList<String> maxRows;

  private AnnotationsQueryService annotationsQueryService;

  private static interface IDisposableObserver<T> extends Disposable, Observer<T> { }
  private IDisposableObserver<List<RowMetaAndData>> pushObserver;

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
    bindStreamingWindowParameters( bindingFactory );
    bindButtons( bindingFactory );
    bindMaxRowsCombo( bindingFactory );
    bindErrorAlert( bindingFactory );
    bindOptImpactInfo( bindingFactory );
  }

  public boolean hideStreaming() {
    return !dataService.isStreaming();
  }

  private void bindButtons( BindingFactory bindingFactory ) throws InvocationTargetException, XulException {
    bindExecutingButton( bindingFactory, (XulButton) document.getElementById( "preview-opt-btn" ) );
    XulButton execButton = (XulButton) document.getElementById( "exec-sql-btn" );
    if ( dataService.isStreaming() ) {
      bindingFactory.createBinding( model, "streaming", execButton, "label", new BindingConvertor<Boolean, String>() {

        @Override
        public String sourceToTarget( Boolean value ) {
          return BaseMessages.getString( PKG,
            value ? "DataServiceTest.Execute.Stop.Button" : "DataServiceTest.Execute.Button" );
        }

        @Override
        public Boolean targetToSource( String value ) {
          return false;
        }
      } );
    } else {
      bindExecutingButton( bindingFactory, execButton );
    }
  }

  /**
   * Binds the button to the executing prop of model, such that the button will be
   * disabled for the duration of a query execution.
   */
  private void bindExecutingButton( BindingFactory bindingFactory, XulButton button ) throws XulException, InvocationTargetException {
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

    if ( !dataService.isStreaming() ) {
      bindingFactory.createBinding( model, "allMaxRows", maxRows, "elements" )
        .fireSourceChanged();
    }
    maxRows.setDisabled( dataService.isStreaming() );
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
    assert document.getElementById( "sql-textbox" ) instanceof XulTextbox;
    XulTextbox sqlTextBox = (XulTextbox) document.getElementById( "sql-textbox" );
    String initSql = getDefaultSql();
    model.setSql( initSql );
    sqlTextBox.setValue( initSql );

    bindingFactory.setBindingType( Binding.Type.BI_DIRECTIONAL );
    bindingFactory.createBinding( model, "sql", sqlTextBox, "value" );
  }

  private void bindStreamingWindowParameters( BindingFactory bindingFactory )
    throws InvocationTargetException, XulException {
    assert document.getElementById( "streaming-groupbox" ) instanceof XulGroupbox;
    assert document.getElementById( "time-based-radio" ) instanceof XulRadio;
    assert document.getElementById( "row-based-radio" ) instanceof XulRadio;
    assert document.getElementById( "window-size" ) instanceof XulTextbox;
    assert document.getElementById( "window-every" ) instanceof XulTextbox;
    assert document.getElementById( "window-limit" ) instanceof XulTextbox;
    assert document.getElementById( "window-size-time-unit" ) instanceof XulLabel;
    assert document.getElementById( "window-every-time-unit" ) instanceof XulLabel;
    assert document.getElementById( "window-limit-time-unit" ) instanceof XulLabel;
    assert document.getElementById( "window-size-row-unit" ) instanceof XulLabel;
    assert document.getElementById( "window-every-row-unit" ) instanceof XulLabel;
    assert document.getElementById( "window-limit-row-unit" ) instanceof XulLabel;
    XulGroupbox streamingGroupBox = (XulGroupbox) document.getElementById( "streaming-groupbox" );
    streamingGroupBox.setVisible( dataService.isStreaming() );

    XulRadio timeBasedRadio = (XulRadio) document.getElementById( "time-based-radio" );
    XulRadio rowBasedRadio = (XulRadio) document.getElementById( "row-based-radio" );
    XulTextbox sizeTextBox = (XulTextbox) document.getElementById( "window-size" );
    XulTextbox everyTextBox = (XulTextbox) document.getElementById( "window-every" );
    XulTextbox limitTextBox = (XulTextbox) document.getElementById( "window-limit" );
    XulLabel sizeTimeUnitLabel = (XulLabel) document.getElementById( "window-size-time-unit" );
    XulLabel everyTimeUnitLabel = (XulLabel) document.getElementById( "window-every-time-unit" );
    XulLabel limitTimeUnitLabel = (XulLabel) document.getElementById( "window-limit-time-unit" );
    XulLabel sizeRowUnitLabel = (XulLabel) document.getElementById( "window-size-row-unit" );
    XulLabel everyRowUnitLabel = (XulLabel) document.getElementById( "window-every-row-unit" );
    XulLabel limitRowUnitLabel = (XulLabel) document.getElementById( "window-limit-row-unit" );

    model.setWindowMode( IDataServiceClientService.StreamingMode.TIME_BASED );

    timeBasedRadio.setSelected( true );
    rowBasedRadio.setSelected( false );

    sizeTimeUnitLabel.setVisible( true );
    everyTimeUnitLabel.setVisible( true );
    limitTimeUnitLabel.setVisible( false );

    sizeRowUnitLabel.setVisible( false );
    everyRowUnitLabel.setVisible( false );
    limitRowUnitLabel.setVisible( true );

    sizeTextBox.setValue( "" );
    everyTextBox.setValue( "" );
    limitTextBox.setValue( "" );

    bindingFactory.setBindingType( Binding.Type.BI_DIRECTIONAL );

    bindingFactory.createBinding( model, "windowSize", sizeTextBox, "value",
      BindingConverters.longToStringEmptyZero() );
    bindingFactory.createBinding( model, "windowEvery", everyTextBox, "value",
      BindingConverters.longToStringEmptyZero() );
    bindingFactory.createBinding( model, "windowLimit", limitTextBox, "value",
      BindingConverters.longToStringEmptyZero() );

    bindingFactory.createBinding( model, "timeBased", timeBasedRadio, "selected" ).fireSourceChanged();
    bindingFactory.createBinding( model, "rowBased", rowBasedRadio, "selected" ).fireSourceChanged();

    bindingFactory.createBinding( timeBasedRadio, "selected", sizeTimeUnitLabel, "visible" );
    bindingFactory.createBinding( timeBasedRadio, "selected", everyTimeUnitLabel, "visible" );
    bindingFactory.createBinding( timeBasedRadio, "!selected", limitTimeUnitLabel, "visible" );
    bindingFactory.createBinding( timeBasedRadio, "!selected", sizeRowUnitLabel, "visible" );
    bindingFactory.createBinding( timeBasedRadio, "!selected", everyRowUnitLabel, "visible" );
    bindingFactory.createBinding( timeBasedRadio, "selected", limitRowUnitLabel, "visible" );

    bindingFactory.createBinding( rowBasedRadio, "!selected", sizeTimeUnitLabel, "visible" );
    bindingFactory.createBinding( rowBasedRadio, "!selected", everyTimeUnitLabel, "visible" );
    bindingFactory.createBinding( rowBasedRadio, "selected", limitTimeUnitLabel, "visible" );
    bindingFactory.createBinding( rowBasedRadio, "selected", sizeRowUnitLabel, "visible" );
    bindingFactory.createBinding( rowBasedRadio, "selected", everyRowUnitLabel, "visible" );
    bindingFactory.createBinding( rowBasedRadio, "!selected", limitRowUnitLabel, "visible" );
  }

  private String getDefaultSql() {
    return "SELECT * FROM \"" + dataService.getName() + "\"";
  }

  public void executeSql() throws KettleException {
    if ( model.isStreaming() ) {
      stopStreaming();
      return;
    }

    resetMetrics();
    dataServiceExec = getNewDataServiceExecutor( true );

    if ( !dataServiceExec.getServiceTrans().isRunning() ) {
      updateOptimizationImpact( dataServiceExec );
    }

    updateModel( dataServiceExec );

    AnnotationsQueryService annotationsQuery = getAnnotationsQueryService();
    Query query;

    if ( dataService.isStreaming() ) {
      query = annotationsQuery.prepareQuery( model.getSql(), model.getWindowMode(),
        model.getWindowSize(), model.getWindowEvery(),
        model.getWindowLimit(), ImmutableMap.<String, String>of() );
    } else {
      query = annotationsQuery.prepareQuery( model.getSql(), model.getMaxRows(), ImmutableMap.<String, String>of() );
    }

    if ( null != query ) {
      writeAnnotations( query );
      handleCompletion( dataServiceExec );
    } else {
      callback.onLogChannelUpdate();
      this.stopQuery = false;
      if ( dataService.isStreaming() ) {
        if ( dataServiceExec.executeStreamingQuery( getDataServicePushObserver(), false ) == null ) {
          handleCompletion( dataServiceExec );
        }
      } else {
        if ( dataServiceExec.executeQuery( getDataServiceObserver() ) != null ) {
          pollForCompletion( dataServiceExec );
        } else {
          handleCompletion( dataServiceExec );
        }
      }
    }
  }

  private void stopStreaming() {
    model.setStreaming( false );
    model.setExecuting( false );
    if ( pushObserver != null ) {
      pushObserver.dispose();
    }
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

              if ( stopQuery || anyTransErrors( dataServiceExec ) || transDone( svcTrans, genTrans ) ) {
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
    return dataService.isStreaming() ? genTrans.isFinished()
      : ( svcTrans.isFinishedOrStopped() && genTrans.isFinishedOrStopped() );
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
    if ( !dataService.isStreaming() ) {
      if ( model.getMaxRows() > 0
        && model.getMaxRows() <= model.getResultRows().size() ) {
        //Exceeded max rows, no need to continue
        stopDataService( dataServiceExec );
      }
    }
  }

  private void stopDataService( DataServiceExecutor dataServiceExec ) {
    dataServiceExec.stop( true );
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
    if ( !dataServiceExec.getServiceTrans().isRunning() ) {
      updateOptimizationImpact( dataServiceExec );
    }
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

      DataServiceExecutor.Builder builder;

      if ( dataService.isStreaming() ) {
        builder = new DataServiceExecutor.Builder( new SQL( model.getSql() ), dataService, context ).
          rowLimit( dataService.getRowLimit() ).
          timeLimit( dataService.getTimeLimit() ).
          logLevel( model.getLogLevel() ).
          enableMetrics( enableMetrics ).
          windowMode( model.getWindowMode() ).
          windowSize( model.getWindowSize() ).
          windowEvery( model.getWindowEvery() ).
          windowLimit( model.getWindowLimit() );
      } else {
        builder = new DataServiceExecutor.Builder( new SQL( model.getSql() ), dataService, context ).
          rowLimit( model.getMaxRows() ).
          logLevel( model.getLogLevel() ).
          enableMetrics( enableMetrics );
      }

      return builder.build();
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

  private IDisposableObserver<List<RowMetaAndData>> getDataServicePushObserver() {
    if ( pushObserver == null ) {
      pushObserver = new IDisposableObserver<List<RowMetaAndData>>() {
        private Disposable toDispose;
        long start;
        final Object rowsMutex = new Object();
        private boolean updating;
        private boolean first;

        @Override
        public void onSubscribe( Disposable d ) {
          toDispose = d;
          document.invokeLater( () -> model.setStreaming( true ) );
          start = System.currentTimeMillis();
          updating = false;
          first = true;
        }

        @Override
        public void onNext( List<RowMetaAndData> rowsList ) {
          synchronized ( rowsMutex ) {
            model.clearResultRows();
            if ( !rowsList.isEmpty() ) {
              model.setResultRowMeta( rowsList.get( 0 ).getRowMeta() );
              rowsList.stream().map( RowMetaAndData::getData ).forEach( model::addResultRow );
            }
            final long batchStart = start;
            start = System.currentTimeMillis();
            if ( !updating ) {
              updating = true;
              document.invokeLater( () -> {
                synchronized ( rowsMutex ) {
                  if ( isDisposed() ) {
                    return;
                  }
                  updateExecutingMessage( batchStart, dataServiceExec );
                  maybeSetErrorAlert( dataServiceExec );
                  if ( first ) {
                    first = false;
                    callback.onExecuteComplete();
                  } else {
                    callback.onUpdate( model.getResultRows() );
                  }
                  updating = false;
                }
              } );
            }
          }
        }

        @Override
        public void onError( Throwable e ) {
          document.invokeLater( () -> model.setStreaming( false ) );
        }

        @Override
        public void onComplete() {
          document.invokeLater( () -> model.setStreaming( false ) );
        }

        @Override
        public void dispose() {
          if ( toDispose != null ) {
            toDispose.dispose();
          }
        }

        @Override
        public boolean isDisposed() {
          return toDispose != null && toDispose.isDisposed();
        }
      };
    }
    return pushObserver;
  }

  private Observer<RowMetaAndData> getDataServiceObserver() {
    PublishSubject<RowMetaAndData> consumer = PublishSubject.create();
    consumer.doOnComplete( () -> {
      stopQuery = true;
    } ).subscribe( rowMetaAndData -> {
      model.addResultRow( rowMetaAndData.getData() );
      // if there were any rows read we want to make sure RowMeta is consistent w/ the
      // actual results, since datatypes may have changed (e.g. with sum(integer_field) )
      model.setResultRowMeta( rowMetaAndData.getRowMeta() );
    } );
    return consumer;
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

      String streamingServiceName = dataService.getName();

      context.removeServiceTransExecutor( streamingServiceName );
    }

    model.setExecuting( false );
    model.setStreaming( false );
    if ( pushObserver != null && !pushObserver.isDisposed() ) {
      pushObserver.dispose();
    }
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

  public void setAnnotationsQueryService( AnnotationsQueryService annotationsQueryService ) {
    this.annotationsQueryService = annotationsQueryService;
  }

  public AnnotationsQueryService getAnnotationsQueryService() {
    if ( null == annotationsQueryService ) {
      annotationsQueryService = new AnnotationsQueryService( new MetastoreLocator() {
        @Override
        public IMetaStore getMetastore( String providerKey ) {
          return null;
        }
        @Override
        public IMetaStore getMetastore() {
          return null;
        }
        @Override public String setEmbeddedMetastore( IMetaStore metastore ) {
          return null;
        }
        @Override public void disposeMetastoreProvider( String providerKey ) {
        }
        @Override public IMetaStore getExplicitMetastore( String providerKey ) {
          return null;
        }
      }, new TestResolver() );
    }
    return annotationsQueryService;
  }

  private void writeAnnotations( Query query ) throws KettleException {
    model.clearResultRows();
    ByteArrayOutputStream annoStream = new ByteArrayOutputStream();
    String annoString = null;
    try {
      query.writeTo( annoStream );
      List<String> annoList = IOUtils.readLines( new ByteArrayInputStream( annoStream.toByteArray() ) );
      int startIndex = annoList.indexOf( "<annotations> " );
      annoString = annoList.subList( startIndex, annoList.size() ).stream().collect( Collectors.joining( "\n" ) );
    } catch ( IOException e ) {
      model.setAlertMessage( BaseMessages.getString( PKG, "DataServiceTest.UnableToRetrieveAnnotations.Message" ) );
      throw new KettleException( BaseMessages.getString( PKG, "DataServiceTest.UnableToRetrieveAnnotations.Message" ) );
    }

    ValueMetaString valueMeta = new ValueMetaString( "annotations" );
    valueMeta.setStorageType( ValueMetaInterface.STORAGE_TYPE_BINARY_STRING );
    valueMeta.setStorageMetadata( new ValueMetaString( "annotations" ) );
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( valueMeta );
    model.setResultRowMeta( rowMeta );

    if ( null != annoString ) {
      Object[] row = null;
      try {
        row = new Object[] { annoString.getBytes( Charset.forName( "UTF-8" ) ) };
      } catch ( NullPointerException e ) {
        model.setAlertMessage( BaseMessages.getString( PKG, "DataServiceTest.UnableToRetrieveAnnotations.Message" ) );
        throw new KettleException( BaseMessages.getString( PKG,
          "DataServiceTest.UnableToRetrieveAnnotations.Message" ) );
      }

      if ( null != row ) {
        model.addResultRow( row );
      }
    }
  }

  class TestResolver implements DataServiceResolver {
    @Override public DataServiceMeta getDataService( String dataServiceName ) {
      return dataService;
    }

    @Override public List<DataServiceMeta> getDataServices( Function<Exception, Void> logger ) {
      return null;
    }

    @Override public List<DataServiceMeta> getDataServices( String dataServiceName, Function<Exception, Void> logger ) {
      return null;
    }

    @Override public List<String> getDataServiceNames() {
      return null;
    }

    @Override public List<String> getDataServiceNames( String dataServiceName ) {
      return null;
    }

    @Override public DataServiceExecutor.Builder createBuilder( SQL sql ) throws KettleException {
      return null;
    }
  }
}
