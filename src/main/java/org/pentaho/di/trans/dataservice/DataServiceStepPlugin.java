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

package org.pentaho.di.trans.dataservice;

import org.pentaho.di.trans.dataservice.optimization.AutoOptimizationService;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.pentaho.di.core.gui.SpoonFactory;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.SpoonLifecycleListener;
import org.pentaho.di.ui.spoon.SpoonPerspective;
import org.pentaho.di.ui.spoon.SpoonPlugin;
import org.pentaho.di.ui.spoon.SpoonPluginCategories;
import org.pentaho.di.ui.spoon.SpoonPluginInterface;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;

import java.util.Enumeration;
import java.util.List;
import java.util.ResourceBundle;

@SpoonPlugin( id = "StepAddDataServicePlugin", image = "" )
@SpoonPluginCategories( { "trans-graph" } )
public class DataServiceStepPlugin implements SpoonPluginInterface {

  private DataServiceStepHandler handler;

  private static Class<?> PKG = DataServiceStepPlugin.class;

  private final ResourceBundle resourceBundle = new ResourceBundle() {
    @Override
    public Enumeration<String> getKeys() {
      return null;
    }

    @Override
    protected Object handleGetObject( String key ) {
      return BaseMessages.getString( PKG, key );
    }
  };

  private XulDomContainer container;
  private final String STEP_ADD_DATA_SERVICE =
    "org/pentaho/di/trans/dataservice/ui/xul/dataservice-overlay.xul";

  public DataServiceStepPlugin( DataServiceMetaStoreUtil metaStoreUtil,
                                List<AutoOptimizationService> autoOptimizationServices,
                                List<PushDownFactory> pushDownFactories ) {
    handler = new DataServiceStepHandler( metaStoreUtil, autoOptimizationServices, pushDownFactories );
  }

  @Override
  public void applyToContainer(
    String category, XulDomContainer container )
    throws XulException {

    this.container = container;
    container.registerClassLoader( getClass().getClassLoader() );
    if ( category.equals( "trans-graph" ) ) {
      container.loadOverlay( STEP_ADD_DATA_SERVICE, resourceBundle );
      container.addEventHandler( handler );
    }
  }

  public void removeFromContainer() throws XulException {
    if ( container == null ) {
      return;
    }
    ( (Spoon) SpoonFactory.getInstance() ).getDisplay().syncExec( new Runnable() {
      public void run() {
        try {
          container.removeOverlay( STEP_ADD_DATA_SERVICE );
        } catch ( XulException e ) {
          e.printStackTrace();
        }
        container.deRegisterClassLoader( DataServiceStepPlugin.class.getClassLoader() );
      }
    } );
  }

  @Override public SpoonLifecycleListener getLifecycleListener() {
    return null;
  }

  @Override public SpoonPerspective getPerspective() {
    return null;
  }
}

