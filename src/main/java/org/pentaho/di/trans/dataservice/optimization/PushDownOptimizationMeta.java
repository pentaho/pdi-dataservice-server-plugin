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

package org.pentaho.di.trans.dataservice.optimization;

import com.google.common.util.concurrent.ListenableFuture;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.persist.MetaStoreElementType;
import org.pentaho.ui.xul.XulEventSource;

import java.beans.PropertyChangeListener;
import java.text.MessageFormat;
import java.util.UUID;

/**
 * @author nhudak
 */
@MetaStoreElementType(
  name = "Push Down Optimization",
  description = "Define opportunities to improve Data Service performance by modifying user transformation execution"
)
public class PushDownOptimizationMeta implements XulEventSource {

  private static final Class<?> PKG = PushDownOptimizationMeta.class;

  public static final String PUSH_DOWN_STEP_NAME = "step_name";

  /**
   * User-defined name for this optimization (required)
   */
  @MetaStoreAttribute
  private String name = UUID.randomUUID().toString();

  /**
   * Name of step being optimized (optional)
   */
  @MetaStoreAttribute( key = PUSH_DOWN_STEP_NAME )
  private String stepName = "";

  /**
   * Optimization Type
   */
  @MetaStoreAttribute
  private PushDownType type;

  @MetaStoreAttribute
  private boolean enabled = true;

  public String getName() {
    return name;
  }

  public void setName( String name ) {
    this.name = name;
  }

  public String getStepName() {
    return stepName;
  }

  public void setStepName( String stepName ) {
    this.stepName = stepName;
  }

  public PushDownType getType() {
    return type;
  }

  public void setType( PushDownType type ) {
    this.type = type;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled( boolean enabled ) {
    this.enabled = enabled;
  }

  public ListenableFuture<Boolean> activate( DataServiceExecutor executor ) {
    return getType().activate( executor, this );
  }

  public OptimizationImpactInfo preview( DataServiceExecutor executor ) {
    final OptimizationImpactInfo preview = getType().preview( executor, this );
    if ( enabled ) {
      return preview;
    } else {
      return new OptimizationImpactInfo( getStepName() ) {
        {
          setModified( false );
          setErrorMsg( preview.getErrorMsg() );
          setQueryBeforeOptimization( preview.getQueryBeforeOptimization() );
        }
        @Override public String getDescription() {
          return "#Optimization is disabled\n" + super.getDescription();
        }
      };
    }
  }

  @Override public String toString() {
    return MessageFormat.format( "PushDownOptimizationMeta'{'name=''{0}'', stepName=''{1}'', type={2}'}'",
      name, stepName, type != null ? type.getClass().getName() : null );
  }

  @Override public void addPropertyChangeListener( PropertyChangeListener propertyChangeListener ) {

  }

  @Override public void removePropertyChangeListener( PropertyChangeListener propertyChangeListener ) {

  }
}
