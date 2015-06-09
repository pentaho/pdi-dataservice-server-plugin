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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.StepAdapter;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;

import java.util.List;

import static com.google.common.base.Predicates.instanceOf;

/**
 * @author nhudak
 */
public class ServiceObserver extends AbstractFuture<CachedService> implements Runnable {
  private final DataServiceExecutor executor;

  public ServiceObserver( DataServiceExecutor executor ) {
    this.executor = executor;
  }

  public ListenableFuture<CachedService> install() {
    List<Runnable> serviceReady = executor.getListenerMap().get( DataServiceExecutor.ExecutionPoint.READY );
    if ( Iterables.any( serviceReady, instanceOf( ServiceObserver.class ) ) ) {
      setException( new IllegalStateException( "More than one cache is configured for this data service." ) );
    } else {
      serviceReady.add( this );
    }
    return this;
  }

  @Override public void run() {
    final List<RowMetaAndData> rowMetaAndData = Lists.newLinkedList();
    StepInterface serviceStep = executor.getServiceTrans().findRunThread( executor.getService().getStepname() );
    serviceStep.addRowListener( new RowAdapter() {
      @Override public synchronized void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) {
        rowMetaAndData.add( new RowMetaAndData( rowMeta, row ) );
      }
    } );
    serviceStep.addStepListener( new StepAdapter() {
      @Override public void stepFinished( Trans trans, StepMeta stepMeta, StepInterface step ) {
        if ( executor.getGenTrans().getErrors() > 0 ) {
          setException(
            new KettleException( "Dynamic transformation finished with errors, could not cache results" ) );
        } else if ( step.isStopped() ) {
          set( CachedService.partial( rowMetaAndData, executor ) );
        } else {
          set( CachedService.complete( rowMetaAndData ) );
        }
      }
    } );
  }
}
