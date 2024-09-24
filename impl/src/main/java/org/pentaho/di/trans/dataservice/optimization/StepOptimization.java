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
import com.google.common.util.concurrent.ListenableFutureTask;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMetaDataCombi;

import java.util.concurrent.Callable;

/**
 * @author nhudak
 */
public abstract class StepOptimization implements PushDownType {

  public ListenableFuture<Boolean> activate( final DataServiceExecutor executor,
                                             final PushDownOptimizationMeta meta ) {
    ListenableFutureTask<Boolean> future = ListenableFutureTask.create( new Callable<Boolean>() {
      @Override public Boolean call() throws Exception {
        StepInterface stepInterface = executor.getServiceTrans().findRunThread( meta.getStepName() );
        return activate( executor, stepInterface );
      }
    } );
    executor.getListenerMap().put( DataServiceExecutor.ExecutionPoint.OPTIMIZE, future );
    return future;
  }

  @Override public OptimizationImpactInfo preview( DataServiceExecutor executor, PushDownOptimizationMeta meta ) {
    OptimizationImpactInfo info = new OptimizationImpactInfo( meta.getStepName() );
    Trans serviceTrans = executor.getServiceTrans();
    try {
      // Start up service Trans threads
      serviceTrans.prepareExecution( null );

      // Find the step thread and run preview
      info = preview( executor, serviceTrans.findRunThread( meta.getStepName() ) );

      // Dispose resources
      for ( StepMetaDataCombi stepMetaDataCombi : serviceTrans.getSteps() ) {
        stepMetaDataCombi.step.setOutputDone();
        stepMetaDataCombi.step.dispose( stepMetaDataCombi.meta, stepMetaDataCombi.data );
        stepMetaDataCombi.step.markStop();
      }
    } catch ( Exception e ) {
      info.setErrorMsg( e );
    }
    return info;
  }

  protected abstract boolean activate( DataServiceExecutor executor, StepInterface stepInterface );

  protected abstract OptimizationImpactInfo preview( DataServiceExecutor executor, StepInterface stepInterface );
}
