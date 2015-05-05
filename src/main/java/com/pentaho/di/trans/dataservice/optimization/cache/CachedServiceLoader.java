package com.pentaho.di.trans.dataservice.optimization.cache;

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.pentaho.di.trans.dataservice.DataServiceExecutor;
import com.pentaho.di.trans.dataservice.execution.DefaultTransWiring;
import com.pentaho.di.trans.dataservice.execution.TransStarter;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.StepAdapter;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author nhudak
 */
class CachedServiceLoader implements Serializable {
  private final List<RowMetaAndData> rowMetaAndData;

  public CachedServiceLoader( List<RowMetaAndData> rowMetaAndData ) {
    this.rowMetaAndData = rowMetaAndData;
  }

  public static CacheKey createCacheKey( DataServiceExecutor executor ) {
    return new CacheKey( executor.getServiceName(), executor.getSql().getWhereClause() );
  }

  public List<RowMetaAndData> getRowMetaAndData() {
    return rowMetaAndData;
  }

  public static ListenableFuture<CachedServiceLoader> observe( StepInterface serviceStep ) {
    final SettableFuture<CachedServiceLoader> future = SettableFuture.create();
    final List<RowMetaAndData> rowMetaAndData = Lists.newLinkedList();
    serviceStep.addRowListener( new RowAdapter() {
      @Override public synchronized void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) {
        rowMetaAndData.add( new RowMetaAndData( rowMeta, row ) );
      }
    } );
    serviceStep.addStepListener( new StepAdapter() {
      @Override public void stepFinished( Trans trans, StepMeta stepMeta, StepInterface step ) {
        if ( step.isStopped() ) {
          future.cancel( false );
        } else {
          future.set( new CachedServiceLoader( rowMetaAndData ) );
        }
      }
    } );
    return future;
  }

  public ListenableFuture<Integer> replay( final DataServiceExecutor executor ) {
    final RowProducer rowProducer;
    final Trans serviceTrans = executor.getServiceTrans();
    final AtomicInteger rowCount = new AtomicInteger( 0 );

    List<Runnable> startTrans = executor.getListenerMap().get( DataServiceExecutor.ExecutionPoint.START ),
      postOptimization = executor.getListenerMap().get( DataServiceExecutor.ExecutionPoint.READY );

    try {
      rowProducer = executor.addRowProducer();
    } catch ( KettleException e ) {
      return Futures.immediateFailedFuture( e );
    }
    final SettableFuture<Integer> future = SettableFuture.create();

    Iterables.removeIf( postOptimization,
      Predicates.instanceOf( DefaultTransWiring.class )
    );
    Iterables.removeIf( startTrans,
      new Predicate<Runnable>() {
        @Override public boolean apply( Runnable runnable ) {
          return runnable instanceof TransStarter && ( (TransStarter) runnable ).getTrans().equals( serviceTrans );
        }
      }
    );
    startTrans.add( new Runnable() {
      @Override public void run() {
        new Thread( executor.getServiceName() + " - Cached Service Loader" ) {
          @Override public void run() {
            serviceTrans.killAll();
            Iterator<RowMetaAndData> iterator = rowMetaAndData.iterator();
            while ( iterator.hasNext() && !future.isDone() ) {
              RowMetaAndData metaAndData = iterator.next();
              rowProducer.putRow( metaAndData.getRowMeta(), metaAndData.getData() );
              rowCount.incrementAndGet();
            }
            rowProducer.finished();
            future.set( rowCount.get() );
          }
        }.start();
      }
    } );
    executor.getGenTrans().findRunThread( executor.getResultStepName() ).addStepListener( new StepAdapter(){
      @Override public void stepFinished( Trans trans, StepMeta stepMeta, StepInterface step ) {
        future.set( rowCount.get() );
      }
    });
    return future;
  }

  static final class CacheKey implements Serializable {
    private final String dataServiceName;
    private final String whereClause;

    public CacheKey( String dataServiceName, String whereClause ) {
      this.dataServiceName = dataServiceName;
      this.whereClause = whereClause;
    }

    @Override public boolean equals( Object o ) {
      if ( this == o ) {
        return true;
      }
      if ( o == null || getClass() != o.getClass() ) {
        return false;
      }
      CacheKey cacheKey = (CacheKey) o;
      return Objects.equal( whereClause, cacheKey.whereClause ) &&
        Objects.equal( dataServiceName, cacheKey.dataServiceName );
    }

    @Override public int hashCode() {
      return Objects.hashCode( dataServiceName, whereClause );
    }

    @Override public String toString() {
      return "CacheKey{dataServiceName='" + dataServiceName + "', whereClause='" + whereClause + "'}";
    }
  }
}
