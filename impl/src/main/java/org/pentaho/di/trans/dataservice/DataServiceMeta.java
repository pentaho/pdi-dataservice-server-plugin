/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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
  public static final String IS_STREAMING = "streaming";
  public static final String ROW_LIMIT = "row_limit";
  public static final String TIME_LIMIT = "time_limit";

  protected String name;

  @MetaStoreAttribute( key = ROW_LIMIT )
  protected int rowLimit;

  @MetaStoreAttribute( key = TIME_LIMIT )
  protected long timeLimit;

  @MetaStoreAttribute( key = DATA_SERVICE_TRANSFORMATION_STEP_NAME )
  protected String stepname;

  @MetaStoreAttribute( key = PUSH_DOWN_OPT_META )
  protected List<PushDownOptimizationMeta> pushDownOptimizationMeta = Lists.newArrayList();

  private TransMeta serviceTrans;

  @MetaStoreAttribute( key = IS_USER_DEFINED )
  protected boolean userDefined = true;

  @MetaStoreAttribute( key = IS_STREAMING )
  protected boolean streaming;

  /**
   * Constructor.
   *
   * @param serviceTrans The service step subject of the data service.
   */
  public DataServiceMeta( TransMeta serviceTrans ) {
    this( serviceTrans, false );
  }

  /**
   * Constructor.
   *
   * @param serviceTrans The service step subject of the data service.
   * @param streaming True if it's a streaming data service, false otherwise.
   */
  public DataServiceMeta( TransMeta serviceTrans, boolean streaming ) {
    this.serviceTrans = serviceTrans;
    this.streaming = streaming;
  }

  // Constructor for MetaStore
  // serviceTrans should otherwise always be given
  @Deprecated
  public DataServiceMeta() {
  }

  /**
   * Getter for the data service name.
   *
   * @return The data service name.
   */
  @Override public String getName() {
    return name;
  }

  /**
   * Setter for the data service name.
   *
   * @param name The data service name.
   */
  @Override public void setName( String name ) {
    this.name = name;
  }

  /**
   * Getter for the {@link org.pentaho.di.trans.TransMeta} data service trans meta.
   *
   * @return The data service trans meta.
   */
  public TransMeta getServiceTrans() {
    return serviceTrans;
  }

  /**
   * Setter for the {@link org.pentaho.di.trans.TransMeta} data service trans meta.
   *
   * @param serviceTrans The {@link org.pentaho.di.trans.TransMeta} data service trans meta to be set.
   */
  public void setServiceTrans( TransMeta serviceTrans ) {
    this.serviceTrans = serviceTrans;
  }

  /**
   * Getter for the data service step name.
   *
   * @return The data service step name.
   */
  public String getStepname() {
    return stepname;
  }

  /**
   * Setter for the data service step name.
   *
   * @param stepname The data service step name.
   */
  public void setStepname( String stepname ) {
    this.stepname = stepname;
  }

  /**
   * Getter for the {@link org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta}
   * data service push down optimizations list.
   *
   * @return The {@link org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta}
   *         data service push down optimizations list.
   */
  public List<PushDownOptimizationMeta> getPushDownOptimizationMeta() {
    return pushDownOptimizationMeta;
  }

  /**
   * Setter for the data service {@link org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta}
   * push down optimizations list.
   *
   * @param pushDownOptimizationMeta The {@link org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta}
   *                 data service push down optimizations list.
   */
  public void setPushDownOptimizationMeta( List<PushDownOptimizationMeta> pushDownOptimizationMeta ) {
    this.pushDownOptimizationMeta = pushDownOptimizationMeta;
  }

  /**
   * Getter for the data service user defined property.
   *
   * @return True if userDefined is set to true, false otherwise.
   */
  public boolean isUserDefined() {
    return userDefined;
  }

  /**
   * Setter for the data service user defined property.
   *
   * @param userDefined True if userDefined is set to true, false otherwise.
   */
  public void setUserDefined( final boolean userDefined ) {
    this.userDefined = userDefined;
  }

  /**
   * Getter for the data service streaming property.
   *
   * @return True if streaming is set to true, false otherwise.
   */
  public boolean isStreaming() {
    return streaming;
  }

  /**
   * Setter for the data service user defined property.
   *
   * @param streaming True if streaming is set to true, false otherwise.
   */
  public void setStreaming( final boolean streaming ) {
    this.streaming = streaming;
  }

  /**
   * Getter for the data service row limit.
   *
   * @return The data service row limit.
   */
  public int getRowLimit() {
    return this.rowLimit;
  }

  /**
   * Setter for the data service row limit.
   *
   * @param rowLimit The new data service row limit.
   */
  public void setRowLimit( int rowLimit ) {
    this.rowLimit = rowLimit;
  }

  /**
   * Getter for the data service time limit for streaming windows (milliseconds).
   *
   * @return The data service time limit for streaming windows in milliseconds.
   */
  public long getTimeLimit() {
    return this.timeLimit;
  }

  /**
   * Setter for the data service time limit.
   *
   * @param timeLimit The new data service time limit.
   */
  public void setTimeLimit( long timeLimit ) {
    this.timeLimit = timeLimit;
  }

  @Override public String toString() {
    return Objects.toStringHelper( this )
      .add( "name", name )
      .add( "serviceTrans", serviceTrans )
      .add( "stepname", stepname )
      .add( "userDefined", userDefined )
      .add( "streaming", streaming )
      .add( "rowLimit", rowLimit )
      .add( "timeLimit", timeLimit )
      .add( "pushDownOptimizationMeta", pushDownOptimizationMeta )
      .toString();
  }
}
