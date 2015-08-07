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
    return MAXROWS_CHOICES.get( maxRows );
  }

  public void setMaxRows( int maxRows ) {
    this.maxRows = maxRows;
  }

  public List<Integer> getAllMaxRows() {
    return MAXROWS_CHOICES;
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
}
