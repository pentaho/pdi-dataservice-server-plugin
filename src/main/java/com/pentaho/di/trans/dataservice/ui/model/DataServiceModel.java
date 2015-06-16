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

package com.pentaho.di.trans.dataservice.ui.model;

import com.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.ui.xul.XulEventSourceAdapter;

import java.util.Collection;
import java.util.List;

public class DataServiceModel extends XulEventSourceAdapter {

  private List<String> steps;
  private String selectedStep;
  private List<PushDownOptimizationMeta> pushDownOptimizations;
  private String serviceName;
  private String transName;
  private String transLocation;

  public String getServiceName() {
    return serviceName;
  }

  public void setServiceName( String serviceName ) {
    this.serviceName = serviceName;
    firePropertyChange( "serviceName", null, serviceName );
  }

  public List<String> getSteps() {
    return steps;
  }

  public void setSteps( List<String> steps ) {
    this.steps = steps;
    firePropertyChange( "steps", null, steps );
  }

  public String getSelectedStep() {
    return selectedStep;
  }

  public void setSelectedStep( String selectedStep ) {
    this.selectedStep = selectedStep;
  }

  public List<PushDownOptimizationMeta> getPushDownOptimizations() {
    return pushDownOptimizations;
  }

  public void setPushDownOptimizations( List<PushDownOptimizationMeta> pushDownOptimizations ) {
    this.pushDownOptimizations = pushDownOptimizations;
    firePropertyChange( "pushDownOptimizations", null, pushDownOptimizations );
  }

  public void addPushDownOptimizations( Collection<PushDownOptimizationMeta> pushDownOptimizations ) {
    this.pushDownOptimizations.addAll( pushDownOptimizations );
    firePropertyChange( "pushDownOptimizations", null, this.pushDownOptimizations );
  }

  public void removePushDownOptimization( PushDownOptimizationMeta pushDownOptimization ) {
    this.pushDownOptimizations.remove( pushDownOptimization );
    firePropertyChange( "pushDownOptimizations", null, this.pushDownOptimizations );
  }

  public String getTransName() {
    return transName;
  }

  public void setTransName( String transName ) {
    this.transName = transName;
    firePropertyChange( "transName", null, transName );
  }

  public String getTransLocation() {
    return transLocation;
  }

  public void setTransLocation( String transLocation ) {
    this.transLocation = transLocation;
    firePropertyChange( "transLocation", null, transLocation );
  }
}
