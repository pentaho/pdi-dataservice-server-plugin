/*!
* Copyright 2010 - 2013 Pentaho Corporation.  All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
*/

package com.pentaho.di.trans.dataservice;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectory;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.IMetaStoreElementType;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.MetaStoreUtil;
import org.pentaho.metastore.util.PentahoDefaults;

import java.util.List;

public class DataServiceMetaStoreUtil extends MetaStoreUtil {

  private static String namespace = PentahoDefaults.NAMESPACE;

  public static final String GROUP_DATA_SERVICE = "DataService";
  public static final String DATA_SERVICE_NAME = "name";
  public static final String DATA_SERVICE_STEPNAME = "stepname";

  public static DataServiceMeta fromTransMeta( TransMeta transMeta, IMetaStore metaStore ) throws MetaStoreException {
    String serviceName = transMeta.getAttribute( GROUP_DATA_SERVICE, DATA_SERVICE_NAME );
    if ( Const.isEmpty( serviceName ) ) {
      return null;
    }
    return new MetaStoreFactory<DataServiceMeta>(  DataServiceMeta.class, metaStore, PentahoDefaults.NAMESPACE )
      .loadElement( serviceName );
  }


  public static void toTransMeta( TransMeta transMeta, IMetaStore metaStore, DataServiceMeta dataService,
                                  boolean saveToMetaStore ) throws MetaStoreException {

    if ( dataService != null && dataService.isDefined() ) {
      // Also make sure the metastore object is stored properly
      //
      transMeta.setAttribute( GROUP_DATA_SERVICE, DATA_SERVICE_NAME, dataService.getName() );
      transMeta.setAttribute( GROUP_DATA_SERVICE, DATA_SERVICE_STEPNAME, dataService.getStepname() );

      LogChannel.GENERAL.logBasic( "Saving data service in meta store '" + transMeta.getMetaStore() + "'" );

      // Leave trace of this transformation...
      //
      Repository repository = transMeta.getRepository();
      if ( transMeta.getRepository() != null ) {
        dataService.setTransRepositoryPath(
          transMeta.getRepositoryDirectory().getPath() + RepositoryDirectory.DIRECTORY_SEPARATOR + transMeta
            .getName() );
        if ( repository.getRepositoryMeta().getRepositoryCapabilities().supportsReferences() ) {
          ObjectId oid = transMeta.getObjectId();
          dataService.setTransObjectId( oid == null ? null : oid.getId() );
        } else {
          dataService
            .setTransRepositoryPath( transMeta.getRepositoryDirectory().getPath() + "/" + transMeta.getName() );
        }
      } else {
        dataService.setTransFilename( transMeta.getFilename() );
      }
      if ( saveToMetaStore ) {
        new MetaStoreFactory<DataServiceMeta>( DataServiceMeta.class, metaStore, namespace )
          .saveElement( dataService );
      }
    }
  }

  /**
   * This method creates a new Element Type in the meta store corresponding encapsulating the Kettle Data Service meta
   * data.
   *
   * @param metaStore The store to create the element type in
   * @return The existing or new element type
   * @throws MetaStoreException
   * @throws org.pentaho.metastore.api.exceptions.MetaStoreNamespaceExistsException
   */
  public static IMetaStoreElementType createDataServiceElementTypeIfNeeded( IMetaStore metaStore )
    throws MetaStoreException {

    if ( !metaStore.namespaceExists( namespace ) ) {
      metaStore.createNamespace( namespace );
    }

    IMetaStoreElementType elementType =
      metaStore.getElementTypeByName( namespace, PentahoDefaults.KETTLE_DATA_SERVICE_ELEMENT_TYPE_NAME );
    if ( elementType == null ) {
      try {
        elementType = metaStore.newElementType( namespace );
        elementType.setName( PentahoDefaults.KETTLE_DATA_SERVICE_ELEMENT_TYPE_NAME );
        elementType.setDescription( PentahoDefaults.KETTLE_DATA_SERVICE_ELEMENT_TYPE_DESCRIPTION );
        metaStore.createElementType( namespace, elementType );
      } catch ( MetaStoreException e ) {
        throw new MetaStoreException( "Unable to create new data service element type in the metastore", e );
      }
    }
    return elementType;
  }

  public static List<DataServiceMeta> getDataServices( IMetaStore metaStore ) throws MetaStoreException {
    return new MetaStoreFactory<DataServiceMeta>( DataServiceMeta.class, metaStore, namespace )
      .getElements();
  }


}
