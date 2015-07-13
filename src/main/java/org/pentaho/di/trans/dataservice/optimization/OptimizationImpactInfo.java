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

package org.pentaho.di.trans.dataservice.optimization;

public class OptimizationImpactInfo {
  private String queryBeforeOptimization = "";
  private String queryAfterOptimization = "";
  private final String stepName;
  private boolean modified = false;
  private String errorMsg = "";

  public OptimizationImpactInfo( String stepName ) {
    this.stepName = stepName;
  }

  public String getQueryBeforeOptimization() {
    return queryBeforeOptimization == null || queryBeforeOptimization.trim().length() == 0
      ? "<no query>" : queryBeforeOptimization;
  }

  public void setQueryBeforeOptimization( String queryBeforeOptimization ) {
    this.queryBeforeOptimization = queryBeforeOptimization;
  }

  public String getQueryAfterOptimization() {
    return queryAfterOptimization;
  }

  public void setQueryAfterOptimization( String queryAfterOptimization ) {
    this.queryAfterOptimization = queryAfterOptimization;
  }

  public String getStepName() {
    return stepName;
  }

  public boolean isModified() {
    return modified;
  }

  public void setModified( boolean modified ) {
    this.modified = modified;
  }

  public String getErrorMsg() {
    return errorMsg;
  }

  public void setErrorMsg( String errorMsg ) {
    this.errorMsg = errorMsg;
  }

  public String getDescription() {
    StringBuilder builder = new StringBuilder();
    builder
      .append( "Step:  " )
      .append( getStepName() )
      .append( "\n" );
    if ( getErrorMsg().length() > 0 ) {
      builder
        .append( "[ERROR]  " )
        .append( getErrorMsg() )
        .append( "\n" );
    }
    if ( isModified() ) {
      builder
        .append( "Before:\n     " )
        .append( getQueryBeforeOptimization() )
        .append( "\nAfter:\n     " )
        .append( getQueryAfterOptimization() );
    } else {
      builder
        .append( "[NO MODIFICATION]\n" )
        .append( "Query:\n     " )
        .append( getQueryBeforeOptimization() );
    }
    return builder.toString();
  }

}
