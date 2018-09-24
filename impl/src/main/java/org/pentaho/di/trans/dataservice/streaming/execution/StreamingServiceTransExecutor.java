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

import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import io.reactivex.functions.Consumer;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.dataservice.Context;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;
import org.pentaho.di.trans.dataservice.execution.PrepareExecution;
import org.pentaho.di.trans.dataservice.execution.TransStarter;
import org.pentaho.di.trans.dataservice.streaming.StreamList;
import org.pentaho.di.trans.dataservice.streaming.StreamServiceKey;
import org.pentaho.di.trans.dataservice.streaming.WindowParametersHelper;
import org.pentaho.di.trans.dataservice.utils.DataServiceConstants;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.StepInterface;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class represents a streaming execution for a service transformation.
 * It spans a thread to run the transformation when data is requested and running thread exists, caching the requests.
 * When all the cached requests are expired the transformation is stopped and it's associated thread terminated.
 */
public class StreamingServiceTransExecutor {
  private final Trans serviceTrans;
  private final StreamServiceKey key;
  private final String serviceStepName;
  private final AtomicBoolean isRunning = new AtomicBoolean( false );
  private final AtomicBoolean isStopping = new AtomicBoolean( false );

  private StreamList<RowMetaAndData> stepStream;
  private long lastCacheCleanupMillis = 0;
  private int windowMaxRowLimit;
  private long windowMaxTimeLimit;
  private Context context;

  /**
   * Service listener cache.
   */
  private final Cache<String, StreamExecutionListener> serviceListeners = CacheBuilder.newBuilder()
    .expireAfterAccess( DataServiceConstants.STREAMING_CACHE_DURATION, DataServiceConstants.STREAMING_CACHE_TIME_UNIT )
    .removalListener( new RemovalListener<String, StreamExecutionListener>() {
      public void onRemoval( RemovalNotification<String, StreamExecutionListener> removal ) {
        LogChannelInterface log = serviceTrans.getLogChannel();

        removal.getValue().unSubscribe();

        //remove the generated trans from the dataservices context cache
        context.removeStreamingGeneratedTransExecution( removal.getKey() );
        if ( serviceListeners.size() == 0 ) {
          context.removeServiceTransExecutor( key );
        }

        log.logDebug( DataServiceConstants.STREAMING_CACHE_REMOVED + removal.getKey() );
      }
    } )
    .softValues()
    .build();

  /**
   * Constructor.
   *
   * @param key The instance key.
   * @param serviceTrans The {@link org.pentaho.di.trans.Trans} Service Transformation.
   * @param serviceStepName The Service Transformation Step Name.
   * @param windowMaxRowLimit The streaming window max row limit.
   * @param windowMaxTimeLimit The streaming window max time limit.
   * @param context The dataservices context.
   */
  public StreamingServiceTransExecutor( final StreamServiceKey key, Trans serviceTrans, final String serviceStepName,
                                        final int windowMaxRowLimit, final long windowMaxTimeLimit,
                                        final Context context ) {
    this.serviceTrans = serviceTrans;
    this.key = key;
    this.serviceStepName = serviceStepName;
    this.windowMaxRowLimit = windowMaxRowLimit;
    this.windowMaxTimeLimit = windowMaxTimeLimit;
    this.context = context;
  }

  /**
   * Getter for the {@link org.pentaho.di.trans.Trans} serviceTransformation
   *
   * @return {@link org.pentaho.di.trans.Trans} serviceTransformation
   */
  public Trans getServiceTrans() {
    return serviceTrans;
  }

  /**
   * Getter for the key.
   *
   * @return the key of this object instance.
   */
  public StreamServiceKey getKey() {
    return key;
  }

  /**
   * Getter for the window max row limit.
   *
   * @return the window max row limit of this object instance.
   */
  public long getWindowMaxRowLimit() {
    return windowMaxRowLimit;
  }

  /**
   * Getter for the window max time limit.
   *
   * @return the window max time limit of this object instance.
   */
  public long getWindowMaxTimeLimit() {
    return windowMaxTimeLimit;
  }

  /**
   * This method is used by the client to get the stream listener fot the given query and window parameters.
   * If no cached listener exists it creates a new one, and spans the Service Transformation execution thread if not
   * started, otherwise it returns the cached listener.
   *
   * @param query The requested query.
   * @param windowConsumer The consumer for the window that is produced by the buffer.
   * @param windowMode The streaming window mode.
   * @param windowSize The query window size. Number of rows for a ROW_BASED streamingType and milliseconds for a
   *                 TIME_BASED streamingType.
   * @param windowEvery The query window rate. Number of rows for a ROW_BASED streamingType and milliseconds for a
   *                 TIME_BASED streamingType.
   * @param windowLimit The query max window size. Number of rows for a TIME_BASED streamingType and milliseconds for a
   *                 ROW_BASED streamingType.
   * @return The {@link StreamExecutionListener} for the given query or null if any parameters are invalid (windowSize equal to 0).
   */
  public StreamExecutionListener getBuffer( String query, Consumer<List<RowMetaAndData>> windowConsumer, final IDataServiceClientService.StreamingMode windowMode,
                                            long windowSize, long windowEvery, long windowLimit ) {

    String cacheId = WindowParametersHelper.getCacheKey( query, windowMode, windowSize, windowEvery, windowMaxRowLimit,
      windowMaxTimeLimit, windowLimit, getKey().hashCode() );

    //this is a special case we want to deal in a graceful way... when this is true
    //an empty output should be produced upstream (the null should ensure that behaviour)
    if ( cacheId == null ) {
      return null;
    }

    StreamExecutionListener streamListener = serviceListeners.getIfPresent( cacheId );

    if ( streamListener == null ) {
      if ( stepStream == null ) {
        stepStream = new StreamList<>();
      }

      boolean timeBased = IDataServiceClientService.StreamingMode.TIME_BASED.equals( windowMode );
      boolean rowBased = IDataServiceClientService.StreamingMode.ROW_BASED.equals( windowMode );

      int maxRows = WindowParametersHelper.getMaxRows( windowMaxRowLimit, windowLimit, timeBased );
      long maxTime = WindowParametersHelper.getMaxTime( windowMaxTimeLimit, windowLimit, rowBased );

      windowSize = WindowParametersHelper.getWindowSize( windowSize, timeBased, maxRows, maxTime );
      windowEvery = WindowParametersHelper.getWindowEvery( windowEvery, timeBased, maxRows, maxTime );

      streamListener = new StreamExecutionListener( stepStream.getStream(), windowConsumer, windowMode, windowSize, windowEvery,
        maxRows, maxTime );

      serviceListeners.put( cacheId, streamListener );
    }

    if ( isRunning.compareAndSet( false, true  ) ) {
      startService();
    }

    return streamListener;
  }

  /**
   * Starts the Service transformation and its row event listener.
   */
  private void startService() {
    lastCacheCleanupMillis = System.currentTimeMillis();

    PrepareExecution prepExec = new PrepareExecution( serviceTrans );
    Runnable serviceTransWiring = getServiceTransWiring();
    TransStarter startTrans = new TransStarter( serviceTrans );

    prepExec.run();
    serviceTransWiring.run();
    startTrans.run();
  }

  /**
   * Sets up the service transformation to be started.
   *
   * @return Runnable to set up the service transformation for running.
   */
  private Runnable getServiceTransWiring() {
    return () -> {
      final Trans trans = serviceTrans;

      StepInterface serviceStep = trans.findRunThread( serviceStepName );
      if ( serviceStep == null ) {
        throw Throwables.propagate( new KettleException( "Service step is not accessible" ) );
      }

      serviceStep.addRowListener( new RowAdapter() {
        /**
         * Listener for the service transformation output rows.
         * If the stepStream has any valid registered listeners it writes the output row to the stepStream,
         * otherwise stops the service transformation and kills its running thread.
         *
         * @param rowMeta The metadata of the written row.
         * @param row The data of the written row.
         * @throws KettleStepException
         */
        @Override
        public void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
          // Add the row to the data stream. If no listeners are registered then the service transformation is stopped.
          LogChannelInterface log = trans.getLogChannel();

          try {
            log.logRowlevel( DataServiceConstants.PASSING_ALONG_ROW + rowMeta.getString( row ) );
          } catch ( KettleValueException e ) {
            // Ignore error
          }

          serviceCacheCleanup();

          if ( serviceListeners.size() > 0 ) {
            RowMetaAndData rowData = new RowMetaAndData();
            rowData.setRowMeta( rowMeta );
            rowData.setData( row );

            stepStream.add( rowData );
          } else {
            stopService();
          }
        }
      } );
    };
  }

  /**
   * Clean up service listener cache.
   */
  private void serviceCacheCleanup() {
    long currentTime = System.currentTimeMillis();
    long updateTime = lastCacheCleanupMillis + ( DataServiceConstants.STREAMING_CACHE_CLEANUP_INTERVAL_SECONDS * 1000 );

    if ( updateTime <= currentTime ) {
      serviceListeners.cleanUp();
      lastCacheCleanupMillis = currentTime;
    }
  }

  /**
   * Clears the listeners cache.
   */
  public void clearCache() {
    serviceListeners.invalidateAll();
    this.serviceListeners.cleanUp();
  }

  /**
   * Clears the listeners cache for a specific listener by cache key.
   * @param key The key to use to remove the listener
   */
  public void clearCacheByKey( String key ) {
    if ( key != null ) {
      serviceListeners.invalidate( key );
      this.serviceListeners.cleanUp();
    }
  }

  /**
   * Stops the transformation.
   */
  private void stopService() {
    if ( isRunning.get() && isStopping.compareAndSet( false, true ) ) {
      LogChannelInterface log = serviceTrans.getLogChannel();

      serviceTrans.stopAll();

      isRunning.set( false );
      isStopping.set( false );

      log.logDetailed( DataServiceConstants.STREAMING_TRANSFORMATION_STOPPED );
    }
  }

  /**
   * Stops the transformation and cleans the cached executions.
   */
  public void stopAll() {
    stopService();
    clearCache();
  }

  /**
   * Get the execution listener and does a touch in it's cache value so that the timeout time is reset.
   *
   * @param key the service listener key.
   * @return the {@see StreamExecutionListener}, or null if doesn't exist
   */
  public StreamExecutionListener touchServiceListener( String key ) {
    return this.serviceListeners.getIfPresent( key );
  }
}
