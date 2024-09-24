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

package org.pentaho.di.trans.dataservice.streaming.execution;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import io.reactivex.BackpressureOverflowStrategy;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Observer;
import io.reactivex.functions.Consumer;
import io.reactivex.subjects.BehaviorSubject;
import io.reactivex.subjects.PublishSubject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;
import org.pentaho.di.trans.dataservice.utils.DataServiceConstants;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.StepInterface;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class to represent the Streaming execution of boundary Generated Transformation (SQL query over dataservices)
 */
public class StreamingGeneratedTransExecution implements Runnable {

  private static final Log logger = LogFactory.getLog( StreamingGeneratedTransExecution.class );

  private final StreamingServiceTransExecutor serviceExecutor;
  private final Trans genTrans;
  private final String injectorStepName;
  private final String resultStepName;
  private final BehaviorSubject<List<RowMetaAndData>> genTransCachePublishSubject = BehaviorSubject.createDefault( new ArrayList<>( ) );
  private final String query;
  private final AtomicBoolean isRunning = new AtomicBoolean( false );
  private StreamExecutionListener stream;
  private final AtomicInteger consumersCount = new AtomicInteger( 0 );

  private final PublishSubject<List<RowMetaAndData>> generatedDataObservable;

  private IDataServiceClientService.StreamingMode windowMode;
  private long windowSize;
  private long windowEvery;
  private long windowLimit;
  private String streamingGeneratedTransCacheKey;

  /**
   * Constructor.
   *
   * @param serviceExecutor The {@link StreamingServiceTransExecutor} service transformation executor object.
   * @param genTrans The {@link org.pentaho.di.trans.Trans} generated transformation.
   * @param rowConsumer The consumer to be registered in the publish subject stream
   * @param injectorStepName The name of the step in the generated transformation where rows are injected.
   * @param resultStepName The name of the step in the generated transformation where the results are retreived.
   * @param query The query to be executed.
   * @param windowMode The streaming window mode.
   * @param windowSize The query window size. Number of rows for boundary ROW_BASED streamingType and milliseconds for boundary
   *                 TIME_BASED streamingType.
   * @param windowEvery The query window rate. Number of rows for boundary ROW_BASED streamingType and milliseconds for boundary
   *                 TIME_BASED streamingType.
   * @param windowLimit The query max window size. Number of rows for boundary TIME_BASED streamingType and milliseconds for boundary
   *                 ROW_BASED streamingType.
   * @param streamingGeneratedTransCacheKey The generated transformation cache key, so that it could be easily accessed.
   */
  public StreamingGeneratedTransExecution( final StreamingServiceTransExecutor serviceExecutor, final Trans genTrans,
                                           final Observer<List<RowMetaAndData>> rowConsumer, boolean pollingMode,
                                           final String injectorStepName, final String resultStepName, final String query,
                                           final IDataServiceClientService.StreamingMode windowMode,
                                           long windowSize, long windowEvery, long windowLimit,
                                           String streamingGeneratedTransCacheKey ) {
    this.serviceExecutor = serviceExecutor;
    this.genTrans = genTrans;
    this.injectorStepName = injectorStepName;
    this.resultStepName = resultStepName;
    this.query = query;
    this.windowMode = windowMode;
    this.windowSize = windowSize;
    this.windowEvery = windowEvery;
    this.windowLimit = windowLimit;
    this.streamingGeneratedTransCacheKey = streamingGeneratedTransCacheKey;

    this.generatedDataObservable = PublishSubject.create();
    this.addNewRowConsumer( rowConsumer, pollingMode );
  }

  /**
   * Gets the window consumer, that should be used to consume the data produced in the service executor.
   * @return A {@link Consumer} that accepts a list of {@link RowMetaAndData} produced by the service executor.
   */
  @VisibleForTesting
  protected Consumer<List<RowMetaAndData>> getWindowConsumer() {
    return rowMetaAndDataList -> getGeneratedDataObservable().onNext( rowMetaAndDataList );
  }

  /**
   * Spans boundary thread to execute the transformation.
   */
  @Override public void run() {
    //we stop back pressure by only processing the last data
    getGeneratedDataObservable()
      .toFlowable( BackpressureStrategy.LATEST )
      .onBackpressureBuffer( 1, () -> { }, BackpressureOverflowStrategy.DROP_OLDEST )
      .doOnError( t -> logger.error( "Error receiving data from the service transformation observable", t ) )
      .doOnNext( rowMetaAndDataList -> {
        if ( consumersCount.get() > 0 ) {
          serviceExecutor.touchServiceListener( this.streamingGeneratedTransCacheKey );
        }
      } )
      .subscribe( rowMetaAndDataList -> this.runGenTrans( rowMetaAndDataList ) );

    // This is where we will inject the rows from the service transformation step
    if ( this.stream == null ) {
      this.stream = serviceExecutor.getBuffer( query, getWindowConsumer(),
        windowMode, windowSize, windowEvery, windowLimit );
    }
    try {
      if ( stream == null ) {
        this.runGenTrans( Collections.emptyList() );
      }
    } catch ( KettleStepException e ) {
      throw Throwables.propagate( e );
    }
  }

  /**
   * Runs the generated transformation with the rows given by param.
   *
   * @param rowIterator The {@link List} input rows.
   * @throws KettleStepException
   */
  @VisibleForTesting
  protected void runGenTrans( final List<RowMetaAndData> rowIterator ) throws KettleStepException {
    if ( isRunning.compareAndSet( false, true ) ) {
      try {
        LogChannelInterface log = genTrans.getLogChannel();
        RowProducer rowProducer;
        StepInterface resultStep;

        genTrans.getTransListeners().clear();
        genTrans.cleanup();
        synchronized ( serviceExecutor.getServiceTrans() ) {
          if ( Thread.currentThread().isInterrupted() ) {
            //avoids InterruptedException caused by the transformations being stopped in the meantime
            return;
          }
          genTrans.prepareExecution( null );
          rowProducer = genTrans.addRowProducer( injectorStepName, 0 );
          genTrans.startThreads();
        }

        resultStep = genTrans.findRunThread( resultStepName );
        resultStep.cleanup();

        List<RowMetaAndData> rowsList = new ArrayList<>( );
        RowAdapter rowListener = getRowWrittenListener( rowsList );
        resultStep.addRowListener( rowListener );
        injectRows( rowIterator, log, rowProducer );

        rowProducer.finished();

        // The execution will not continue until the trans finishes)
        waitForGeneratedTransToFinnish();

        genTrans.stopAll();
        resultStep.removeRowListener( rowListener );

        if ( !this.genTransCachePublishSubject.hasComplete() ) {
          this.genTransCachePublishSubject.onNext( Collections.unmodifiableList( rowsList ) );
        }

        log.logDetailed( DataServiceConstants.STREAMING_GENERATED_TRANSFORMATION_STOPPED );
      } catch ( KettleException e ) {
        throw new KettleStepException( e );
      } finally {
        isRunning.set( false );
        genTrans.setRunning( false );
        genTrans.setStopped( true );
      }
    }
  }

  @VisibleForTesting
  protected void waitForGeneratedTransToFinnish() {
    try {
      genTrans.waitUntilFinished();
    } catch ( RuntimeException e ) {
      if ( e.getCause() instanceof InterruptedException ) {
        logger.debug( "The generated transformation was stopped, which resulted in an interruption of the generated transformation wait until finished call" );
        //this is normal if the transformation is terminated while we are waiting for it to finnish
      } else {
        throw new RuntimeException( e );
      }
    }
  }

  @VisibleForTesting
  protected RowAdapter getRowWrittenListener( List<RowMetaAndData> tempList ) {
    return new RowAdapter() {
      @Override public void rowWrittenEvent( RowMetaInterface rowMetaInterface, Object[] objects ) throws KettleStepException {
        tempList.add( new RowMetaAndData( rowMetaInterface, objects ) );
      }
    };
  }

  @VisibleForTesting
  protected void injectRows( List<RowMetaAndData> rowIterator, LogChannelInterface log, RowProducer rowProducer ) {
    for ( RowMetaAndData injectRows : rowIterator ) {
      while ( !rowProducer.putRowWait( injectRows.getRowMeta(), injectRows.getData(), 1, TimeUnit.SECONDS )
        && genTrans.isRunning() ) {
        // Row queue was full, try again
        log.logRowlevel( DataServiceConstants.ROW_BUFFER_IS_FULL_TRYING_AGAIN );
      }
    }
  }

  /**
   * Adds a new consumer that will receive the result of the generated transformation
   * @param consumer The consumer to register.
   * @param pollingMode True if the new consumer is a polling consumer, False if it is a push one.
   */
  public void addNewRowConsumer( final Observer<List<RowMetaAndData>> consumer, boolean pollingMode ) {
    if ( pollingMode ) {
      this.genTransCachePublishSubject.safeSubscribe( consumer );
    } else {
      this.genTransCachePublishSubject.doOnDispose( () -> consumersCount.decrementAndGet() ).safeSubscribe( consumer );
      consumersCount.incrementAndGet();
    }
  }

  /**
   * Clears all the row consumers from the cache, and wrap-up resources used by them.
   */
  public void clearRowConsumers( ) {
    this.genTransCachePublishSubject.onComplete();
    consumersCount.set( 0 );
  }

  @VisibleForTesting
  protected PublishSubject<List<RowMetaAndData>> getGeneratedDataObservable() {
    return generatedDataObservable;
  }
}
