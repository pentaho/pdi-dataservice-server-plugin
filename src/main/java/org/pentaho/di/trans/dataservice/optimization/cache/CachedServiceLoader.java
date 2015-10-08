/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.dataservice.optimization.cache;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.execution.DefaultTransWiring;
import org.pentaho.di.trans.dataservice.execution.TransStarter;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.step.StepMetaDataCombi;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.instanceOf;

/**
 * @author nhudak
 */
class CachedServiceLoader {
  private final Executor executor;
  private final CachedService cachedService;

  CachedServiceLoader( CachedService cachedService, Executor executor ) {
    this.cachedService = cachedService;
    this.executor = executor;
  }

  public ListenableFuture<Integer> replay( DataServiceExecutor dataServiceExecutor ) throws KettleException {
    final Trans serviceTrans = dataServiceExecutor.getServiceTrans(), genTrans = dataServiceExecutor.getGenTrans();
    final CountDownLatch startReplay = new CountDownLatch( 1 );
    final RowProducer rowProducer = dataServiceExecutor.addRowProducer();

    List<Runnable> startTrans = dataServiceExecutor.getListenerMap().get( DataServiceExecutor.ExecutionPoint.START ),
      postOptimization = dataServiceExecutor.getListenerMap().get( DataServiceExecutor.ExecutionPoint.READY );

    Iterables.removeIf( postOptimization,
      instanceOf( DefaultTransWiring.class )
    );
    Iterables.removeIf( startTrans,
      new Predicate<Runnable>() {
        @Override public boolean apply( Runnable runnable ) {
          return runnable instanceof TransStarter && ( (TransStarter) runnable ).getTrans().equals( serviceTrans );
        }
      }
    );

    postOptimization.add( new Runnable() {
      @Override public void run() {
        serviceTrans.stopAll();
        for ( StepMetaDataCombi stepMetaDataCombi : serviceTrans.getSteps() ) {
          stepMetaDataCombi.step.setOutputDone();
          stepMetaDataCombi.step.dispose( stepMetaDataCombi.meta, stepMetaDataCombi.data );
          stepMetaDataCombi.step.markStop();
        }
      }
    } );

    startTrans.add( new Runnable() {
      @Override public void run() {
        startReplay.countDown();
      }
    } );

    ListenableFutureTask<Integer> replay = ListenableFutureTask.create( new Callable<Integer>() {
      @Override public Integer call() throws Exception {
        checkState( startReplay.await( 30, TimeUnit.SECONDS ), "Cache replay did not start" );
        int rowCount = 0;
        for ( Iterator<RowMetaAndData> iterator = cachedService.getRowMetaAndData().iterator();
              iterator.hasNext() && genTrans.isRunning(); ) {
          RowMetaAndData metaAndData = iterator.next();
          boolean rowAdded = false;
          while ( !rowAdded && genTrans.isRunning() ) {
            rowAdded =
              rowProducer.putRowWait( metaAndData.getRowMeta(), metaAndData.getData(), 10, TimeUnit.SECONDS );
          }
          if ( rowAdded ) {
            rowCount += 1;
          }
        }
        rowProducer.finished();
        return rowCount;
      }
    } );
    executor.execute( replay );
    return replay;
  }
}
