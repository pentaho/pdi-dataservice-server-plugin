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


package org.pentaho.di.trans.dataservice.ui.model;

import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;
import org.pentaho.di.trans.dataservice.optimization.OptimizationImpactInfo;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.ui.xul.XulEventSourceAdapter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataServiceTestModel extends XulEventSourceAdapter {
  private String sql;

  protected static final LogLevel DEFAULT_LOGLEVEL = LogLevel.DETAILED;
  private LogLevel logLevel = DEFAULT_LOGLEVEL;

  private String alertMessage;

  private List<OptimizationImpactInfo> optimizationImpact = new ArrayList<OptimizationImpactInfo>();

  private List<Object[]> resultRows = new ArrayList<Object[]>();
  private RowMetaInterface resultRowMeta;

  private LogChannelInterface serviceTransLogChannel;
  private LogChannelInterface genTransLogChannel;

  private int maxRows = 0;

  public static final List<Integer> MAXROWS_CHOICES = Arrays.asList( 100, 500, 1000 );

  private IDataServiceClientService.StreamingMode windowMode = IDataServiceClientService.StreamingMode.TIME_BASED;
  private long windowSize = 0;
  private long windowEvery = 0;
  private long windowLimit = 0;

  private boolean executing = false;
  private boolean streaming = false;

  public String getSql() {
    return sql;
  }

  public void setSql( String sql ) {
    this.sql = sql;
  }

  public LogChannelInterface getServiceTransLogChannel() {
    return serviceTransLogChannel;
  }

  public void setServiceTransLogChannel( LogChannelInterface serviceTransLogChannel ) {
    this.serviceTransLogChannel = serviceTransLogChannel;
  }

  public void setGenTransLogChannel( LogChannelInterface logChannel ) {
    this.genTransLogChannel = logChannel;
  }

  public LogChannelInterface getGenTransLogChannel() {
    return genTransLogChannel;
  }

  public void addResultRow( Object[] row ) {
    resultRows.add( row );
  }

  public List<Object[]> getResultRows() {
    return resultRows;
  }

  public void clearResultRows() {
    resultRows.clear();
  }

  public LogLevel getLogLevel() {
    return logLevel;
  }

  public void setLogLevel( LogLevel logLevel ) {
    this.logLevel = logLevel;
  }

  public List<String> getAllLogLevels() {
    return Arrays.asList( LogLevel.logLevelDescriptions );
  }

  public int getMaxRows() {
    return MAXROWS_CHOICES.get( this.maxRows );
  }

  public void setMaxRows( int maxRows ) {
    this.maxRows = maxRows;
  }

  public List<Integer> getAllMaxRows() {
    return MAXROWS_CHOICES;
  }

  public IDataServiceClientService.StreamingMode getWindowMode() {
    return windowMode;
  }

  public void setWindowMode( IDataServiceClientService.StreamingMode windowMode ) {
    this.windowMode = windowMode;
  }

  public long getWindowSize() {
    return windowSize;
  }

  public void setWindowSize( long windowSize ) {
    this.windowSize = windowSize;
  }

  public long getWindowEvery() {
    return windowEvery;
  }

  public void setWindowEvery( long windowEvery ) {
    this.windowEvery = windowEvery;
  }

  public long getWindowLimit() {
    return windowLimit;
  }

  public void setWindowLimit( long windowLimit ) {
    this.windowLimit = windowLimit;
  }

  public boolean isTimeBased() {
    return IDataServiceClientService.StreamingMode.TIME_BASED.equals( windowMode );
  }

  public void setTimeBased( boolean timeBased ) {
    if ( timeBased ) {
      this.windowMode = IDataServiceClientService.StreamingMode.TIME_BASED;
    } else {
      this.windowMode = IDataServiceClientService.StreamingMode.ROW_BASED;
    }
  }

  public boolean isRowBased() {
    return IDataServiceClientService.StreamingMode.ROW_BASED.equals( windowMode );
  }

  public void setRowBased( boolean rowBased ) {
    if ( rowBased ) {
      this.windowMode = IDataServiceClientService.StreamingMode.ROW_BASED;
    } else {
      this.windowMode = IDataServiceClientService.StreamingMode.TIME_BASED;
    }
  }

  public String getAlertMessage() {
    return alertMessage;
  }

  public void setAlertMessage( String alertMessage ) {
    this.alertMessage = alertMessage;
    firePropertyChange( "alertMessage", null, alertMessage );
  }

  public RowMetaInterface getResultRowMeta() {
    return resultRowMeta;
  }

  public void setResultRowMeta( RowMetaInterface resultRowMeta ) {
    this.resultRowMeta = resultRowMeta;
  }

  public void clearOptimizationImpact() {
    optimizationImpact.clear();
    firePropertyChange( "optimizationImpactDescription", null, getOptimizationImpactDescription() );
  }

  public void addOptimizationImpact( OptimizationImpactInfo info ) {
    optimizationImpact.add( info );
    firePropertyChange( "optimizationImpactDescription", null, getOptimizationImpactDescription() );
  }

  public String getOptimizationImpactDescription() {
    StringBuilder builder = new StringBuilder();
    builder.append( "\n" );
    if ( optimizationImpact.size() == 0 ) {
      builder.append( "[No Push Down Optimizations Defined]\n" );
    }
    for ( OptimizationImpactInfo info : optimizationImpact ) {
      builder.append( info.getDescription() );
      builder.append( "\n- - - - - - - - - - - - - - - - - - - - - -\n\n" );
    }
    return builder.toString();
  }

  public boolean isExecuting() {
    return executing;
  }

  public void setExecuting( boolean executing ) {
    this.executing = executing;
    firePropertyChange( "executing", null, executing );
  }

  public boolean isStreaming() {
    return streaming;
  }

  public void setStreaming( boolean streaming ) {
    boolean previous = this.streaming;
    this.streaming = streaming;
    firePropertyChange( "streaming", previous, streaming );
  }

}
