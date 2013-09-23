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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.sql.ServiceCacheMethod;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectory;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.IMetaStoreElement;
import org.pentaho.metastore.api.IMetaStoreElementType;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.api.exceptions.MetaStoreNamespaceExistsException;
import org.pentaho.metastore.util.MetaStoreUtil;
import org.pentaho.metastore.util.PentahoDefaults;

public class DataServiceMetaStoreUtil extends MetaStoreUtil {
  
  private static String namespace = PentahoDefaults.NAMESPACE;
  
  public final static String GROUP_DATA_SERVICE = "DataService";
  public final static String DATA_SERVICE_NAME = "name";
  public final static String DATA_SERVICE_STEPNAME = "stepname";
  
  public static DataServiceMeta fromTransMeta(TransMeta transMeta, IMetaStore metaStore) throws MetaStoreException {
    String serviceName = transMeta.getAttribute(GROUP_DATA_SERVICE, DATA_SERVICE_NAME);
    if (Const.isEmpty(serviceName)) {
      return null;
    }
    DataServiceMeta meta = new DataServiceMeta();
    return loadDataService(metaStore, serviceName, meta);
  }
  

  public static void toTransMeta(TransMeta transMeta, IMetaStore metaStore, DataServiceMeta dataService, boolean saveToMetaStore) throws MetaStoreException {
    
    // Also make sure the metastore object is stored properly
    //
    transMeta.setAttribute(GROUP_DATA_SERVICE, DATA_SERVICE_NAME, dataService.getName());
    transMeta.setAttribute(GROUP_DATA_SERVICE, DATA_SERVICE_STEPNAME, dataService.getStepname());
      
    if (dataService!=null && dataService.isDefined()) {
      
      LogChannel.GENERAL.logBasic("Saving data service in meta store '"+transMeta.getMetaStore()+"'");

      // Leave trace of this transformation...
      //
      Repository repository = transMeta.getRepository(); 
      if (transMeta.getRepository()!=null) {
        dataService.setTransRepositoryPath(transMeta.getRepositoryDirectory().getPath()+RepositoryDirectory.DIRECTORY_SEPARATOR+transMeta.getName());
        if (repository.getRepositoryMeta().getRepositoryCapabilities().supportsReferences()) {
          dataService.setTransObjectId(transMeta.getObjectId());
        } else {
          dataService.setTransRepositoryPath(transMeta.getRepositoryDirectory().getPath()+"/"+transMeta.getName());
        }
      } else {
        dataService.setTransFilename(transMeta.getFilename());
      }
      if (saveToMetaStore) {
        createOrUpdateDataServiceElement(metaStore, dataService);
      }
    }
  }
  
  /**
   * This method creates a new Element Type in the meta store corresponding encapsulating the Kettle Data Service meta data. 
   * @param metaStore The store to create the element type in
   * @return The existing or new element type
   * @throws MetaStoreException 
   * @throws MetaStoreNamespaceExistsException 
   */
  public static IMetaStoreElementType createDataServiceElementTypeIfNeeded(IMetaStore metaStore) throws MetaStoreException {
    
    if (!metaStore.namespaceExists(namespace)) {
      metaStore.createNamespace(namespace);
    }
    
    IMetaStoreElementType elementType = metaStore.getElementTypeByName(namespace, PentahoDefaults.KETTLE_DATA_SERVICE_ELEMENT_TYPE_NAME);
    if (elementType==null) {
      try {
        elementType = metaStore.newElementType(namespace);
        elementType.setName(PentahoDefaults.KETTLE_DATA_SERVICE_ELEMENT_TYPE_NAME);
        elementType.setDescription(PentahoDefaults.KETTLE_DATA_SERVICE_ELEMENT_TYPE_DESCRIPTION);
        metaStore.createElementType(namespace, elementType);
      } catch(MetaStoreException e) {
        throw new MetaStoreException("Unable to create new data service element type in the metastore", e);
      }
    }
    return elementType;
  }
  
  public static IMetaStoreElement createOrUpdateDataServiceElement(IMetaStore metaStore, DataServiceMeta dataServiceMeta) throws MetaStoreException {
    
    IMetaStoreElementType elementType = createDataServiceElementTypeIfNeeded(metaStore);
    
    IMetaStoreElement oldElement = metaStore.getElementByName(namespace, elementType, dataServiceMeta.getName());
    IMetaStoreElement element = metaStore.newElement(); 
    populateDataServiceElement(metaStore, element, dataServiceMeta);

    if (oldElement==null) {
      metaStore.createElement(namespace, elementType, element);
    } else {
      metaStore.updateElement(namespace, elementType, oldElement.getId(), element);
    }
    
    return element;
  }

  private static void populateDataServiceElement(IMetaStore metaStore, IMetaStoreElement element, DataServiceMeta dataServiceMeta) throws MetaStoreException {
    element.setName(dataServiceMeta.getName());
    element.addChild(metaStore.newAttribute(DataServiceMeta.DATA_SERVICE_STEPNAME, dataServiceMeta.getStepname()));
    element.addChild(metaStore.newAttribute(DataServiceMeta.DATA_SERVICE_TRANSFORMATION_FILENAME, dataServiceMeta.getTransFilename()));
    element.addChild(metaStore.newAttribute(DataServiceMeta.DATA_SERVICE_TRANSFORMATION_REP_PATH, dataServiceMeta.getTransRepositoryPath()));
    element.addChild(metaStore.newAttribute(DataServiceMeta.DATA_SERVICE_TRANSFORMATION_REP_OBJECT_ID, dataServiceMeta.getTransObjectId()));
    element.addChild(metaStore.newAttribute(DataServiceMeta.DATA_SERVICE_TRANSFORMATION_REP_OBJECT_ID, dataServiceMeta.getTransObjectId()));
    
  }

  public static DataServiceMeta loadDataService(IMetaStore metaStore, String dataServiceName, DataServiceMeta meta) throws MetaStoreException {
    IMetaStoreElementType elementType = createDataServiceElementTypeIfNeeded(metaStore);
    IMetaStoreElement element = metaStore.getElementByName(namespace, elementType, dataServiceName);
    if (element!=null) {
      return elementToDataService(element, meta);
    } 
    
    throw new MetaStoreException("Data service '"+dataServiceName+"' could not be found");
 }
  
  private static DataServiceMeta elementToDataService(IMetaStoreElement element) {
    DataServiceMeta dataService = new DataServiceMeta();
    return elementToDataService(element, dataService);
  }
  
  private static DataServiceMeta elementToDataService(IMetaStoreElement element, DataServiceMeta meta) {
    meta.setName(element.getName());
    meta.setStepname(getChildString(element, DataServiceMeta.DATA_SERVICE_STEPNAME));
    meta.setTransFilename(getChildString(element, DataServiceMeta.DATA_SERVICE_TRANSFORMATION_FILENAME));
    meta.setTransRepositoryPath(getChildString(element, DataServiceMeta.DATA_SERVICE_TRANSFORMATION_REP_PATH));
    String transObjectIdString = getChildString(element, DataServiceMeta.DATA_SERVICE_TRANSFORMATION_REP_OBJECT_ID);
    meta.setTransObjectId(StringUtils.isEmpty(transObjectIdString) ? null : new StringObjectId(transObjectIdString));
    meta.setCacheMaxAgeMinutes(NumberUtils.toInt(getChildString(element, DataServiceMeta.DATA_SERVICE_CACHE_MAX_AGE_MINUTES), 0));
    meta.setCacheMethod(ServiceCacheMethod.getMethodByName(getChildString(element, DataServiceMeta.DATA_SERVICE_CACHE_METHOD)));
    return meta;
  }

  public static List<DataServiceMeta> getDataServices(IMetaStore metaStore) throws MetaStoreException {
    IMetaStoreElementType elementType = createDataServiceElementTypeIfNeeded(metaStore);
    List<DataServiceMeta> dataServices = new ArrayList<DataServiceMeta>();
    for (IMetaStoreElement element : metaStore.getElements(namespace,  elementType)) {
      dataServices.add(elementToDataService(element));
    }
    return dataServices;
  }

  
}
