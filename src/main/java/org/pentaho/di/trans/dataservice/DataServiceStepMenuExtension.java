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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.trans.StepMenuExtension;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.ui.xul.containers.XulMenupopup;

@ExtensionPoint( id = "DataServiceStepMenuExtension", description = "Creates popup menus for data services",
  extensionPointId = "TransStepRightClick" )
public class DataServiceStepMenuExtension implements ExtensionPointInterface {

  private static final Log logger = LogFactory.getLog( DataServiceStepMenuExtension.class );

  private DataServiceMetaStoreUtil metaStoreUtil;

  public DataServiceStepMenuExtension( DataServiceMetaStoreUtil metaStoreUtil ) {
    this.metaStoreUtil = metaStoreUtil;
  }

  @Override public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {
    StepMenuExtension extension = (StepMenuExtension) object;
    TransMeta transMeta = extension.getTransGraph().getTransMeta();
    StepMeta stepMeta = extension.getTransGraph().getCurrentStep();
    XulMenupopup menu = extension.getMenu();

    if ( transMeta.getRepository() != null ) {
      Boolean isSaved = transMeta.getObjectId() != null || transMeta.getRepositoryDirectory() != null;
      menu.getElementById( "dataservices-new" ).setDisabled( !isSaved );
    }

    Boolean hasDataService = false;
    try {
      DataServiceMeta dataServiceMeta =
        metaStoreUtil.fromTransMeta( transMeta, Spoon.getInstance().getMetaStore(), stepMeta.getName() );
      hasDataService = dataServiceMeta != null;
    } catch ( MetaStoreException e ) {
      logger.error( "Unable to load data service", e );
    }

    menu.getElementById( "dataservices-edit" ).setDisabled( !hasDataService );
    menu.getElementById( "dataservices-delete" ).setDisabled( !hasDataService );
    menu.getElementById( "dataservices-test" ).setDisabled( !hasDataService );
  }

}
