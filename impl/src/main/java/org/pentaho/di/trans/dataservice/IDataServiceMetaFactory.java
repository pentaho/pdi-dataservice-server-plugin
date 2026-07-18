/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/



package org.pentaho.di.trans.dataservice;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.pentaho.di.trans.step.StepMeta;

public interface IDataServiceMetaFactory {
  public DataServiceMeta createDataService( StepMeta step ) throws KettleException;
  public DataServiceMeta createDataService( StepMeta step, Integer rowLimit ) throws KettleException;
  public PushDownFactory getCacheFactory();
  public void setCacheFactory( PushDownFactory cacheFactory );
}
