/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.serialization.MetaStoreElement;
import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.persist.MetaStoreElementType;
import org.pentaho.metastore.util.PentahoDefaults;

import java.util.List;

/**
 * This describes a (transformation) data service to the outside world.
 * It defines the name, picks the step to read from (or to write to), the caching method etc.
 *
 * @author matt
 */
@MetaStoreElementType(
  name = PentahoDefaults.KETTLE_DATA_SERVICE_ELEMENT_TYPE_NAME,
  description = PentahoDefaults.KETTLE_DATA_SERVICE_ELEMENT_TYPE_DESCRIPTION )
public class DataServiceMeta implements MetaStoreElement {

  public static final String DATA_SERVICE_TRANSFORMATION_STEP_NAME = "step_name";
  public static final String PUSH_DOWN_OPT_META = "push_down_opt_meta";
  public static final String IS_USER_DEFINED = "is_user_defined";

  protected String name;

  @MetaStoreAttribute( key = DATA_SERVICE_TRANSFORMATION_STEP_NAME )
  protected String stepname;

  @MetaStoreAttribute( key = PUSH_DOWN_OPT_META )
  protected List<PushDownOptimizationMeta> pushDownOptimizationMeta = Lists.newArrayList();

  private TransMeta serviceTrans;

  @MetaStoreAttribute( key = IS_USER_DEFINED )
  protected boolean userDefined = true;

  public DataServiceMeta( TransMeta serviceTrans ) {
    this.serviceTrans = serviceTrans;
  }

  // Constructor for MetaStore
  // serviceTrans should otherwise always be given
  @Deprecated
  public DataServiceMeta() {
  }

  /**
   * @return the name
   */
  @Override public String getName() {
    return name;
  }

  /**
   * @param name the name to set
   */
  @Override public void setName( String name ) {
    this.name = name;
  }

  public TransMeta getServiceTrans() {
    return serviceTrans;
  }

  public void setServiceTrans( TransMeta serviceTrans ) {
    this.serviceTrans = serviceTrans;
  }

  /**
   * @return the stepname
   */
  public String getStepname() {
    return stepname;
  }

  /**
   * @param stepname the stepname to set
   */
  public void setStepname( String stepname ) {
    this.stepname = stepname;
  }

  public List<PushDownOptimizationMeta> getPushDownOptimizationMeta() {
    return pushDownOptimizationMeta;
  }

  public void setPushDownOptimizationMeta( List<PushDownOptimizationMeta> pushDownOptimizationMeta ) {
    this.pushDownOptimizationMeta = pushDownOptimizationMeta;
  }

  public boolean isUserDefined() {
    return userDefined;
  }

  public void setUserDefined( final boolean userDefined ) {
    this.userDefined = userDefined;
  }

  @Override public String toString() {
    return Objects.toStringHelper( this )
      .add( "name", name )
      .add( "serviceTrans", serviceTrans )
      .add( "stepname", stepname )
      .add( "userDefined", userDefined )
      .add( "pushDownOptimizationMeta", pushDownOptimizationMeta )
      .toString();
  }
}
