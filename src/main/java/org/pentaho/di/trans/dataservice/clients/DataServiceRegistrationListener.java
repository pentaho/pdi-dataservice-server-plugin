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

package org.pentaho.di.trans.dataservice.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.shared.SharedObjects;

import java.util.Map;

public class DataServiceRegistrationListener {

  private static Log logger = LogFactory.getLog( DataServiceRegistrationListener.class );

  public void register( DataServiceClient dataServiceClient, Map properties ) {
    try {
      SharedObjects sharedObjects = new SharedObjects();
      DatabaseMeta dbMeta = sharedObjects.getSharedDatabase( "Data Services" );
      if ( dbMeta == null ) {
        dbMeta =
          new DatabaseMeta( "Data Services", "KettleThin",
            DatabaseMeta.getAccessTypeDesc( DatabaseMeta.TYPE_ACCESS_NATIVE ),
            "localhost", "default", "0000", "", "" );

        dbMeta.addExtraOption( "KettleThin", "local", "true" );
        dbMeta.setReadOnly( true );

        sharedObjects.storeObject( dbMeta );
        sharedObjects.saveToFile();
      }
    } catch ( Exception e ) {
      logger.error( "Unable to create shared local connection" );
    }
  }

  public void unregister( DataServiceClient dataServiceClient, Map properties ) {
    try {
      SharedObjects sharedObjects = new SharedObjects();
      DatabaseMeta dbMeta = sharedObjects.getSharedDatabase( "Data Services" );
      if ( dbMeta != null ) {
        sharedObjects.removeObject( dbMeta );
        sharedObjects.saveToFile();
      }
    } catch ( Exception e ) {
      logger.error( "Unable to remove shared local connection" );
    }
  }
}
