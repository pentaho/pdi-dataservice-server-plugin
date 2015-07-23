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

package org.pentaho.di.trans.dataservice;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.pentaho.di.core.Const;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.serialization.ServiceTrans;
import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.persist.MetaStoreElementType;
import org.pentaho.metastore.util.PentahoDefaults;

import java.util.List;
import java.util.Set;

/**
 * This describes a (transformation) data service to the outside world.
 * It defines the name, picks the step to read from (or to write to), the caching method etc.
 *
 * @author matt
 */
@MetaStoreElementType(
  name = PentahoDefaults.KETTLE_DATA_SERVICE_ELEMENT_TYPE_NAME,
  description = PentahoDefaults.KETTLE_DATA_SERVICE_ELEMENT_TYPE_DESCRIPTION )
public class DataServiceMeta {

  public static final String DATA_SERVICE_TRANSFORMATION_STEP_NAME = "step_name";
  public static final String PUSH_DOWN_OPT_META = "push_down_opt_meta";

  protected String name;

  @MetaStoreAttribute( key = DATA_SERVICE_TRANSFORMATION_STEP_NAME )
  protected String stepname;

  @MetaStoreAttribute( key = PUSH_DOWN_OPT_META )
  protected List<PushDownOptimizationMeta> pushDownOptimizationMeta = Lists.newArrayList();

  private TransMeta serviceTrans;

  public DataServiceMeta( TransMeta serviceTrans ) {
    this.serviceTrans = serviceTrans;
  }

  // Constructor for MetaStore
  // serviceTrans should otherwise always be given
  @Deprecated
  public DataServiceMeta() {
  }

  public boolean isDefined() {
    return !Const.isEmpty( name ) && !Const.isEmpty( stepname );
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name the name to set
   */
  public void setName( String name ) {
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

  public Set<String> createCacheKeys() {
    return ImmutableSet.of( name, createCacheKey( getServiceTrans(), getStepname() ) );
  }

  public static String createCacheKey( TransMeta transMeta, String stepName ) {
    return String.valueOf( Objects.hashCode( ServiceTrans.reference( transMeta ).getLocation(), stepName ) );
  }
}
