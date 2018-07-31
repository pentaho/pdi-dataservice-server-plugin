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
import io.reactivex.Observable;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Consumer;
import io.reactivex.subjects.PublishSubject;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Class to represents a listener for a service transformation stream.
 */
public class StreamExecutionListener {
  private IDataServiceClientService.StreamingMode windowMode;
  private long windowSize;
  private long windowEvery;
  private int maxRows;
  private long maxTime;
  private Disposable outputSubject;
  private Disposable starterSubject;
  private Disposable subject;
  private Disposable fallbackSubject;
  private Observable<List<RowMetaAndData>> buffer;
  private Observable<List<RowMetaAndData>> fallbackBuffer;
  private List<RowMetaAndData> cachePreWindow = Collections.synchronizedList( new ArrayList<RowMetaAndData>() );
  private PublishSubject<List<RowMetaAndData>> outputBufferPublisher;
  private final AtomicBoolean hasWindow = new AtomicBoolean( false );

  /**
   * Constructor. Subscribes a listener to the given window buffer.
   *
   * @param stream         The {@link io.reactivex.subjects.PublishSubject} data stream.
   * @param windowConsumer The consumer for the windows produced.
   * @param windowMode     The streaming window mode.
   * @param windowSize     The window size. Number of rows for a ROW_BASED streamingType and milliseconds for a
   *                       TIME_BASED streamingType.
   * @param windowEvery    The window rate. Number of rows for a ROW_BASED streamingType and milliseconds for a
   *                       TIME_BASED streamingType.
   * @param maxRows        The max rows window size.
   * @param maxTime        The max time window size.
   */
  public StreamExecutionListener( final PublishSubject<RowMetaAndData> stream,
                                  Consumer<List<RowMetaAndData>> windowConsumer,
                                  final IDataServiceClientService.StreamingMode windowMode, final long windowSize,
                                  final long windowEvery, final int maxRows, final long maxTime ) {
    this.windowMode = windowMode;
    this.windowSize = windowSize;
    this.windowEvery = windowEvery;
    this.maxRows = maxRows;
    this.maxTime = maxTime;

    init( stream, windowConsumer );
  }

  /**
   * Inits the listener streaming buffers.
   *
   * @param stream         The {@link io.reactivex.subjects.PublishSubject} data stream.
   * @param windowConsumer The consumer for the windows produced.
   */
  private void init( PublishSubject<RowMetaAndData> stream, Consumer<List<RowMetaAndData>> windowConsumer ) {
    boolean rowBased = IDataServiceClientService.StreamingMode.ROW_BASED.equals( windowMode );
    boolean timeBased = IDataServiceClientService.StreamingMode.TIME_BASED.equals( windowMode );

    if ( windowEvery > 0 ) {
      if ( timeBased ) {
        this.buffer = stream.buffer( windowSize, windowEvery, TimeUnit.MILLISECONDS );
      } else if ( rowBased ) {
        this.buffer = stream.buffer( (int) windowSize, (int) windowEvery );
      }
    } else if ( timeBased ) {
      this.buffer = stream.buffer( windowSize, TimeUnit.MILLISECONDS );
    } else {
      this.buffer = stream.buffer( (int) windowSize );
    }
    this.fallbackBuffer = stream.buffer( maxTime, TimeUnit.MILLISECONDS, maxRows );

    this.outputBufferPublisher = PublishSubject.create();
    this.outputSubject = this.outputBufferPublisher.subscribe( windowConsumer );

    this.subject = this.buffer.subscribe( this::processBufferWindow );
    this.fallbackSubject = this.fallbackBuffer.subscribe( this::processFallbackWindow );

    // below is created the streaming objects used while the first window is not produced
    if ( timeBased ) {
      this.starterSubject = stream.buffer( (int) ( windowEvery > 0 ? windowEvery : windowSize ), TimeUnit.MILLISECONDS )
        .subscribe( items -> {
          if ( !hasWindow.get() ) {
            this.cachePreWindow.addAll( items );
            outputBufferPublisher.onNext( this.cachePreWindow );
          }
        } );
    } else {
      this.starterSubject = stream.buffer( (int) ( windowEvery > 0 ? windowEvery : windowSize ) )
        .subscribe( items -> {
          if ( !hasWindow.get() ) {
            this.cachePreWindow.addAll( items );
            outputBufferPublisher.onNext( this.cachePreWindow );
          }
        } );
    }
  }

  /**
   * Getter for the cached window data collected before a window is ready.
   *
   * @return The {@link List} cached window data.
   */
  public List<RowMetaAndData> getCachePreWindow() {
    return this.cachePreWindow;
  }

  /**
   * Un-subscribes the streaming buffers.
   */
  public void unSubscribe() {
    unSubscribeOutput();
    unSubscribeStarter();
    unSubscribeBuffer();
    unSubscribeFallbackBuffer();
  }

  /**
   * Un-subscribes the output streaming buffer.
   */
  @VisibleForTesting
  protected void unSubscribeOutput() {
    if ( this.outputSubject != null && !this.outputSubject.isDisposed() ) {
      this.outputSubject.dispose();
    }
    this.outputSubject = null;
  }

  /**
   * Un-subscribes the starter streaming buffer.
   */
  @VisibleForTesting
  protected void unSubscribeStarter() {
    if ( this.starterSubject != null && !this.starterSubject.isDisposed() ) {
      this.starterSubject.dispose();
    }
    this.starterSubject = null;
    this.cachePreWindow.clear();
  }

  /**
   * Un-subscribes the streaming buffer.
   */
  @VisibleForTesting
  protected void unSubscribeBuffer() {
    if ( this.subject != null && !this.subject.isDisposed() ) {
      this.subject.dispose();
    }
    this.subject = null;
  }

  /**
   * Un-subscribes the streaming fallback buffer.
   */
  @VisibleForTesting
  protected void unSubscribeFallbackBuffer() {
    if ( this.fallbackSubject != null && !this.fallbackSubject.isDisposed() ) {
      this.fallbackSubject.dispose();
    }
    this.fallbackSubject = null;
  }

  /**
   * Processes a buffer data window.
   *
   * @return The {@link io.reactivex.Observable} window list to process.
   */
  private void processBufferWindow( List<RowMetaAndData> windowList ) {
    //When the observable is complete it means that a window was produced
    synchronized ( this.buffer ) {
      if ( hasWindow.compareAndSet( false, true ) ) {
        unSubscribeStarter();
      }

      //If we are processing a regular window buffer, we should discard the fallback one
      unSubscribeFallbackBuffer();
      this.fallbackSubject = this.fallbackBuffer.subscribe( this::processFallbackWindow );

      if ( this.outputSubject != null && !this.outputSubject.isDisposed() ) {
        outputBufferPublisher.onNext( windowList );
      }
    }
  }

  /**
   * Processes a fallback buffer data window.
   *
   * @return The {@link io.reactivex.Observable} fallback window list to process.
   */
  private void processFallbackWindow( List<RowMetaAndData> windowList ) {
    //When the observable is complete it means that a window was produced
    synchronized ( this.buffer ) {

      if ( hasWindow.compareAndSet( false, true ) ) {
        unSubscribeStarter();
      }

      //If we are processing a fallback window buffer, we should discard the regular one
      unSubscribeBuffer();
      this.subject = this.buffer.subscribe( this::processBufferWindow );

      if ( this.outputSubject != null && !this.outputSubject.isDisposed() ) {
        outputBufferPublisher.onNext( windowList );
      }
    }
  }
}
