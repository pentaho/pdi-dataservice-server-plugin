/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2015 Pentaho Corporation (Pentaho). All rights reserved.
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

package com.pentaho.di.trans.dataservice.optimization.cache;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.pentaho.di.trans.dataservice.DataServiceExecutor;
import com.pentaho.di.trans.dataservice.execution.DefaultTransWiring;
import com.pentaho.di.trans.dataservice.execution.TransStarter;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.Trans;

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
  final private Executor executor;
  final private CachedService cachedService;

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
        serviceTrans.killAll();
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
