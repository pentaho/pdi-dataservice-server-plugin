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
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.dataservice.Context;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;
import org.pentaho.di.trans.dataservice.execution.CopyParameters;
import org.pentaho.di.trans.dataservice.execution.PrepareExecution;
import org.pentaho.di.trans.dataservice.execution.TransStarter;
import org.pentaho.di.trans.dataservice.streaming.StreamList;
import org.pentaho.di.trans.dataservice.streaming.StreamServiceKey;
import org.pentaho.di.trans.dataservice.utils.DataServiceConstants;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.StepInterface;

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
   * @param windowMode The streaming window mode.
   * @param windowSize The query window size. Number of rows for a ROW_BASED streamingType and milliseconds for a
   *                 TIME_BASED streamingType.
   * @param windowEvery The query window rate. Number of rows for a ROW_BASED streamingType and milliseconds for a
   *                 TIME_BASED streamingType.
   * @param windowLimit The query max window size. Number of rows for a TIME_BASED streamingType and milliseconds for a
   *                 ROW_BASED streamingType.
   * @return The {@link StreamExecutionListener} for the given query or null if parameters invalid.
   */
  public StreamExecutionListener getBuffer( String query, final IDataServiceClientService.StreamingMode windowMode,
                                            long windowSize, long windowEvery, long windowLimit ) {
    boolean rowBased = IDataServiceClientService.StreamingMode.ROW_BASED.equals( windowMode );
    boolean timeBased = IDataServiceClientService.StreamingMode.TIME_BASED.equals( windowMode );

    int maxRows = windowLimit > 0 && timeBased ? (int) Math.min( windowLimit, windowMaxRowLimit )
      : windowMaxRowLimit;

    long maxTime = windowLimit > 0 && rowBased ? Math.min( windowLimit, windowMaxTimeLimit )
      : windowMaxTimeLimit;

    windowSize = windowSize <= 0 ? 0
      : ( timeBased ? Math.min( windowSize, maxTime ) : Math.min( windowSize, maxRows ) );
    windowEvery = windowEvery <= 0 ? 0
      : ( timeBased ? Math.min( windowEvery, maxTime ) : Math.min( windowEvery, maxRows ) );

    if ( windowSize == 0 ) {
      return null;
    }

    String cacheId = getCacheKey( query, windowMode, windowSize, windowEvery, maxRows,
      maxTime );

    StreamExecutionListener streamListener = serviceListeners.getIfPresent( cacheId );

    if ( streamListener == null ) {
      if ( stepStream == null ) {
        stepStream = new StreamList<>();
      }

      streamListener = new StreamExecutionListener( stepStream.getStream(), windowMode, windowSize, windowEvery,
        maxRows, maxTime );

      serviceListeners.put( cacheId, streamListener );
    }

    if ( isRunning.compareAndSet( false, true  ) ) {
      startService();
    }

    return streamListener;
  }

  /**
   * Generates the cache key for a given query with a specific size and rate.
   *
   * @param query The query.
   * @param mode The query window mode.
   * @param size The query window size.
   * @param every The query window rate.
   * @param maxRows The query window max rows.
   * @param maxTime The query window max time.
   * @return The cache key for the query.
   */
  private String getCacheKey( String query, IDataServiceClientService.StreamingMode mode, long size, long every,
                              int maxRows, long maxTime ) {
    return query.concat( mode.toString() ).concat( "-" ).concat( String.valueOf( size ) )
      .concat( "-" ).concat( String.valueOf( every ) )
      .concat( "-" ).concat( String.valueOf( maxRows ) )
      .concat( "-" ).concat( String.valueOf( maxTime ) );
  }

  /**
   * Starts the Service transformation and its row event listener.
   */
  private void startService() {
    lastCacheCleanupMillis = System.currentTimeMillis();

    //Copy parameters into the service transformation
    DataServiceExecutor dataServiceExecutor = context.getExecutor( serviceTrans.getContainerObjectId() );
    if ( dataServiceExecutor != null ) {
      new CopyParameters( dataServiceExecutor.getParameters(), serviceTrans ).run();
    }

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
    serviceListeners.cleanUp();
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
}
