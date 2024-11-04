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


package org.pentaho.di.trans.dataservice.optimization.cache;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.StepAdapter;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Predicates.instanceOf;

/**
 * @author nhudak
 */
public class ServiceObserver extends AbstractFuture<CachedService> implements Runnable {
  private final DataServiceExecutor executor;

  List<RowMetaAndData> rowMetaAndData = Lists.newArrayList();
  boolean isRunning = true;
  CountDownLatch latch = new CountDownLatch( 1 );

  public ServiceObserver( DataServiceExecutor executor ) {
    this.executor = executor;
  }

  public Iterator<RowMetaAndData> rows() {
    return new Iterator<RowMetaAndData>() {
      int index = 0;
      @Override public boolean hasNext() {
        if ( rowMetaAndData.size() > index ) {
          return true;
        }
        if ( isRunning ) {
          latch = new CountDownLatch( 1 );
          try {
            while ( !latch.await( 1, TimeUnit.SECONDS ) ) {
              if ( !isRunning ) {
                return rowMetaAndData.size() > index;
              }
            }
            return rowMetaAndData.size() > index;
          } catch ( InterruptedException e ) {
            return rowMetaAndData.size() > index;
          }
        } else {
          return rowMetaAndData.size() > index;
        }
      }

      @Override public RowMetaAndData next() {
        return rowMetaAndData.get( index++ );
      }
    };
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
    StepInterface serviceStep = executor.getServiceTrans().findRunThread( executor.getService().getStepname() );
    serviceStep.addRowListener( new RowAdapter() {
      @Override public synchronized void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) {
        Object[] clonedRow;
        try {
          clonedRow = rowMeta.cloneRow( row );
        } catch ( KettleValueException e ) {
          setException( e );
          return;
        }
        rowMetaAndData.add( new RowMetaAndData( rowMeta, clonedRow ) );
        latch.countDown();
      }
    } );
    serviceStep.addStepListener( new StepAdapter() {
      @Override public void stepFinished( Trans trans, StepMeta stepMeta, StepInterface step ) {
        isRunning = false;
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
