/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
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
