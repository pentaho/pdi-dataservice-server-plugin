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

package com.pentaho.di.trans.dataservice;

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
