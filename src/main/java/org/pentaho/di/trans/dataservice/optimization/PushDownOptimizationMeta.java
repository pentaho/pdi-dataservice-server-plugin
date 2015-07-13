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

import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.persist.MetaStoreElementType;
import org.pentaho.ui.xul.XulEventSource;

import java.beans.PropertyChangeListener;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

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

  private static List<String> statuses;

  /**
   * User-defined name for this optimization (required)
   */
  @MetaStoreAttribute
  private String name = "";

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

  public String getTypeName() {
    return type.getTypeName();
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled( boolean enabled ) {
    this.enabled = enabled;
  }

  public void setStatus( String status ) {
    if ( status.equals( BaseMessages.getString( PKG, "ParamGenOptForm.Enabled.Label" ) ) ) {
      enabled = true;
    } else {
      enabled = false;
    }
  }

  public String getStatus() {
    if ( enabled ) {
      return BaseMessages.getString( PKG, "ParamGenOptForm.Enabled.Label" );
    }

    return BaseMessages.getString( PKG, "ParamGenOptForm.Disabled.Label" );
  }

  public static Vector<String> getStatuses() {
    if ( statuses == null || statuses.size() == 0 ) {
      statuses = new ArrayList<String>();
      statuses.add( BaseMessages.getString( PKG, "ParamGenOptForm.Enabled.Label" ) );
      statuses.add( BaseMessages.getString( PKG, "ParamGenOptForm.Disabled.Label" ) );
    }
    return new Vector<String>( statuses );
  }

  public boolean activate( DataServiceExecutor executor ) {
    StepInterface stepInterface = executor.getServiceTrans().findRunThread( getStepName() );
    return getType().activate( executor, stepInterface );
  }

  public OptimizationImpactInfo preview( DataServiceExecutor executor ) {
    StepInterface stepInterface = executor.getServiceTrans().findRunThread( getStepName() );
    final OptimizationImpactInfo preview = getType().preview( executor, stepInterface );
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
          return "# " + getName() + " is disabled\n" + super.getDescription();
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
