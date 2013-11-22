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
