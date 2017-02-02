/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
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
            latch.await( 1, TimeUnit.SECONDS );
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
