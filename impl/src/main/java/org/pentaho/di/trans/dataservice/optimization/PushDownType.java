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


package org.pentaho.di.trans.dataservice.optimization;

import com.google.common.util.concurrent.ListenableFuture;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.TransMeta;

/**
 * @author nhudak
 */
public interface PushDownType {

  void init( TransMeta transMeta, DataServiceMeta dataService, PushDownOptimizationMeta optMeta );

  ListenableFuture<Boolean> activate( DataServiceExecutor executor, PushDownOptimizationMeta meta );

  OptimizationImpactInfo preview( DataServiceExecutor executor, PushDownOptimizationMeta meta );
}
