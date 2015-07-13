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

package org.pentaho.di.trans.dataservice.clients;

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
