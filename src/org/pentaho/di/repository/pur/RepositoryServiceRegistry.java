/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2014 Pentaho Corporation (Pentaho). All rights reserved.
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

package org.pentaho.di.repository.pur;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.pentaho.di.repository.IRepositoryService;

public class RepositoryServiceRegistry {
  private final Map<Class<? extends IRepositoryService>, IRepositoryService> serviceMap;
  private final List<Class<? extends IRepositoryService>> serviceList;

  public RepositoryServiceRegistry() {
    serviceMap = new HashMap<Class<? extends IRepositoryService>, IRepositoryService>();
    serviceList = new ArrayList<Class<? extends IRepositoryService>>();
  }

  public void registerService( Class<? extends IRepositoryService> clazz, IRepositoryService service ) {
    if ( serviceMap.put( clazz, service ) == null ) {
      serviceList.add( clazz );
    }
  }

  public IRepositoryService getService( Class<? extends IRepositoryService> clazz ) {
    return serviceMap.get( clazz );
  }

  public List<Class<? extends IRepositoryService>> getRegisteredInterfaces() {
    return serviceList;
  }
}
