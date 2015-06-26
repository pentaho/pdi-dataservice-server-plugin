package org.pentaho.di.trans.dataservice;

import org.pentaho.di.trans.dataservice.cache.DataServiceMetaCache;
import org.pentaho.di.trans.dataservice.optimization.AutoOptimizationService;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.pentaho.di.trans.dataservice.serialization.DataServiceMetaStoreUtil;

import java.util.List;

public class DataServiceContext {
  private final DataServiceMetaStoreUtil metaStoreUtil;
  private final List<AutoOptimizationService> autoOptimizationServices;
  private final List<PushDownFactory> pushDownFactories;

  public DataServiceContext( List<PushDownFactory> pushDownFactories,
                             List<AutoOptimizationService> autoOptimizationServices,
                             DataServiceMetaCache cache ) {
    this.pushDownFactories = pushDownFactories;
    this.autoOptimizationServices = autoOptimizationServices;
    this.metaStoreUtil = new DataServiceMetaStoreUtil( pushDownFactories, cache );
  }

  public DataServiceMetaStoreUtil getMetaStoreUtil() {
    return metaStoreUtil;
  }

  public List<AutoOptimizationService> getAutoOptimizationServices() {
    return autoOptimizationServices;
  }

  public List<PushDownFactory> getPushDownFactories() {
    return pushDownFactories;
  }
}
