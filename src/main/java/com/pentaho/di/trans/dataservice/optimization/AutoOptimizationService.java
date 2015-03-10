package com.pentaho.di.trans.dataservice.optimization;

import com.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.TransMeta;

import java.util.Collection;

/**
 * @author nhudak
 */
public interface AutoOptimizationService {
  public Collection<PushDownOptimizationMeta> apply( TransMeta transMeta, DataServiceMeta dataServiceMeta );
  public Collection<Class<? extends PushDownType>> getProvidedOptimizationTypes();
}
