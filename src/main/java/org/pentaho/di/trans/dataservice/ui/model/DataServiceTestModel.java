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

package org.pentaho.di.trans.dataservice.ui.model;

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

  private static final LogLevel DEFAULT_LOGLEVEL = LogLevel.DETAILED;
  private LogLevel logLevel = DEFAULT_LOGLEVEL;

  private String alertMessage;

  private List<OptimizationImpactInfo> optimizationImpact = new ArrayList<OptimizationImpactInfo>();

  private List<Object[]> resultRows = new ArrayList<Object[]>();
  private RowMetaInterface resultRowMeta;

  private LogChannelInterface serviceTransLogChannel;
  private LogChannelInterface genTransLogChannel;

  private static final int DEFAULT_MAXROWS = 100;
  private int maxRows = DEFAULT_MAXROWS;
  private boolean executing = false;

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
    return maxRows;
  }

  public void setMaxRows( int maxRows ) {
    this.maxRows = maxRows;
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
      builder.append( "[No Push Down Optimizations Defined]" );
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
}
