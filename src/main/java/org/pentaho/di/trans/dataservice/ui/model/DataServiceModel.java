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

import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
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
