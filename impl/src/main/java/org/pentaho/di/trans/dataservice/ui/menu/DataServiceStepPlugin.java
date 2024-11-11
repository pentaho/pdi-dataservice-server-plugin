/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package org.pentaho.di.trans.dataservice.ui.menu;

import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.spoon.SpoonLifecycleListener;
import org.pentaho.di.ui.spoon.SpoonPerspective;
import org.pentaho.di.ui.spoon.SpoonPlugin;
import org.pentaho.di.ui.spoon.SpoonPluginCategories;
import org.pentaho.di.ui.spoon.SpoonPluginInterface;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;

import java.util.Enumeration;
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

  public DataServiceStepPlugin( DataServiceContext context ) {
    handler = new DataServiceStepHandler( context );
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

  @Override public SpoonLifecycleListener getLifecycleListener() {
    return null;
  }

  @Override public SpoonPerspective getPerspective() {
    return null;
  }
}

