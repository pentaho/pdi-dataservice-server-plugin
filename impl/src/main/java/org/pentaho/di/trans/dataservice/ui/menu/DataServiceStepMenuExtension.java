/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.di.trans.dataservice.ui.menu;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.serialization.DataServiceMetaStoreUtil;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.spoon.trans.StepMenuExtension;
import org.pentaho.ui.xul.containers.XulMenupopup;

@ExtensionPoint( id = "DataServiceStepMenuExtension", description = "Creates popup menus for data services",
  extensionPointId = "TransStepRightClick" )
public class DataServiceStepMenuExtension implements ExtensionPointInterface {
  private DataServiceMetaStoreUtil metaStoreUtil;

  public DataServiceStepMenuExtension( DataServiceContext context ) {
    this.metaStoreUtil = context.getMetaStoreUtil();
  }

  @Override public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {
    StepMenuExtension extension = (StepMenuExtension) object;
    TransMeta transMeta = extension.getTransGraph().getTransMeta();
    StepMeta stepMeta = extension.getTransGraph().getCurrentStep();
    XulMenupopup menu = extension.getMenu();

    DataServiceMeta dataService = metaStoreUtil.getDataServiceByStepName( transMeta, stepMeta.getName() );
    Boolean hasDataService = dataService != null && dataService.isUserDefined();

    menu.getElementById( "dataservices-new" ).setDisabled( hasDataService );
    menu.getElementById( "dataservices-edit" ).setDisabled( !hasDataService );
    menu.getElementById( "dataservices-delete" ).setDisabled( !hasDataService );
    menu.getElementById( "dataservices-test" ).setDisabled( !hasDataService );
  }
}
