/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
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

/**
 * Class to represent the Streaming execution of boundary Generated Transformation (SQL query over dataservices)
 */
public class StreamingGeneratedTransExecution implements Runnable {

  private static final Log logger = LogFactory.getLog( StreamingGeneratedTransExecution.class );

  private final StreamingServiceTransExecutor serviceExecutor;
  private final Trans genTrans;
  private final String injectorStepName;
  private final String resultStepName;
  private BehaviorSubject<List<RowMetaAndData>> genTransCachePublishSubject = BehaviorSubject.create();
  private final String query;
  private final AtomicBoolean isRunning = new AtomicBoolean( false );
  private final List<Observer<List<RowMetaAndData>>> consumersList = Collections.synchronizedList( new ArrayList<>() );
  private StreamExecutionListener stream;

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
    getGeneratedDataObservable().
      toFlowable( BackpressureStrategy.LATEST ).
      onBackpressureBuffer( 1, () -> { }, BackpressureOverflowStrategy.DROP_OLDEST )
      .doOnError( t -> logger.error( "Error receiving data from the service transformation observable", t ) )
      .doOnNext( rowMetaAndDataList -> {
        if ( this.consumersList.size() != 0 ) {
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
    if  ( isRunning.compareAndSet( false, true ) ) {
      try {
        LogChannelInterface log = genTrans.getLogChannel();
        RowProducer rowProducer;
        StepInterface resultStep;
        //create a replay subject, so that new added consumers, can replay the last results obtained from the generated transformation
        this.genTransCachePublishSubject = BehaviorSubject.create( );

        this.consumersList.stream().forEach( observer -> this.genTransCachePublishSubject.defaultIfEmpty( new ArrayList<>( ) ).
          doOnDispose( () -> this.consumersList.remove( observer ) ).
          safeSubscribe( observer ) );

        genTrans.getTransListeners().clear();
        genTrans.cleanup();
        genTrans.prepareExecution( null );

        rowProducer = genTrans.addRowProducer( injectorStepName, 0 );

        genTrans.startThreads();

        resultStep = genTrans.findRunThread( resultStepName );

        resultStep.cleanup();
        List<RowMetaAndData> tempList = new ArrayList<>( );
        RowAdapter rowListener = new RowAdapter() {
          @Override public void rowWrittenEvent( RowMetaInterface rowMetaInterface, Object[] objects ) throws KettleStepException {
            tempList.add( new RowMetaAndData( rowMetaInterface, objects ) );
          }
        };
        resultStep.addRowListener( rowListener );

        for ( RowMetaAndData injectRows : rowIterator ) {
          while ( !rowProducer.putRowWait( injectRows.getRowMeta(), injectRows.getData(), 1, TimeUnit.SECONDS )
            && genTrans.isRunning() ) {
            // Row queue was full, try again
            log.logRowlevel( DataServiceConstants.ROW_BUFFER_IS_FULL_TRYING_AGAIN );
          }
        }
        rowProducer.finished();
        genTrans.waitUntilFinished();
        genTrans.stopAll();
        resultStep.removeRowListener( rowListener );

        if ( !StreamingGeneratedTransExecution.this.genTransCachePublishSubject.hasComplete() ) {
          StreamingGeneratedTransExecution.this.genTransCachePublishSubject.onNext( Collections.unmodifiableList( tempList ) );
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

  /**
   * Adds a new consumer that will receive the result of the generated transformation
   * @param consumer The consumer to register.
   * @param pollingMode True if the new consumer is a polling consumer, False if it is a push one.
   */
  public void addNewRowConsumer( final Observer<List<RowMetaAndData>> consumer, boolean pollingMode ) {
    if ( pollingMode ) {
      this.genTransCachePublishSubject.defaultIfEmpty( new ArrayList<>( ) ).safeSubscribe( consumer );
    } else {
      this.consumersList.add( consumer );
    }

  }

  /**
   * Clears all the row consumers from the cache, and wrap-up resources used by them.
   */
  public void clearRowConsumers( ) {
    this.genTransCachePublishSubject.onComplete();
    this.genTransCachePublishSubject = BehaviorSubject.create( );

    this.consumersList.stream().forEach( Observer::onComplete );
    this.consumersList.clear();
  }

  /**
   * Clear a specific consumer from the cache, and wrap up resources used by it.
   * @param consumer
   */
  public void clearRowConsumer( final Observer<List<RowMetaAndData>> consumer ) {
    this.consumersList.remove( consumer );
    if ( consumer != null ) {
      consumer.onComplete();
    }
  }

  @VisibleForTesting
  protected PublishSubject<List<RowMetaAndData>> getGeneratedDataObservable() {
    return generatedDataObservable;
  }
}
