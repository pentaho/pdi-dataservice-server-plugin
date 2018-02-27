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
import org.pentaho.di.core.RowMetaAndData;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Class to represents a listener for a service transformation stream.
 */
public class StreamExecutionListener {
  private Disposable subject;
  private Disposable fallbackSubject;
  private Observable<List<RowMetaAndData>> buffer;
  private Observable<List<RowMetaAndData>> fallbackBuffer;
  private List<RowMetaAndData> cachedWindow = Collections.synchronizedList( new ArrayList<RowMetaAndData>() );
  private final AtomicBoolean isProcessing = new AtomicBoolean( false );

  /**
   * Constructor.
   * Subscribes a listener to the given window buffer.
   *
   * @param buffer The {@link io.reactivex.Observable} stream window buffer.
   * @param fallbackBuffer The {@link io.reactivex.Observable} stream window fallback buffer.
   */
  public StreamExecutionListener( final Observable<List<RowMetaAndData>> buffer,
                                  final Observable<List<RowMetaAndData>> fallbackBuffer ) {
    this.buffer = buffer;

    this.subject = this.buffer.subscribe( list -> {
      processBufferWindow( list );
    } );

    if ( fallbackBuffer != null ) {
      this.fallbackBuffer = fallbackBuffer;

      this.fallbackSubject = this.fallbackBuffer.subscribe( list -> {
        processFallbackWindow( list );
      } );
    }
  }

  /**
   * Checks if there is a cached window data.
   *
   * @return True if theres cached window data, false otherwise.
   */
  public boolean hasCachedWindow() {
    return !this.cachedWindow.isEmpty();
  }

  /**
   * Getter for the cached window data.
   *
   * @return The {@link List} cached window data.
   */
  public List<RowMetaAndData> getCachedWindow() {
    return this.cachedWindow;
  }

  /**
   * Getter for the window buffer.
   *
   * @return The {@link io.reactivex.Observable} window buffer.
   */
  public Observable<List<RowMetaAndData>> getBuffer() {
    return this.buffer;
  }

  /**
   * Getter for the window fallback buffer.
   *
   * @return The {@link io.reactivex.Observable} window fallback buffer.
   */
  public Observable<List<RowMetaAndData>> getFallbackBuffer() {
    return this.fallbackBuffer;
  }

  /**
   * Un-subscribes the streaming buffers.
   */
  public void unSubscribe() {
    unSubscribeBuffer();
    unSubscribeFallbackBuffer();
  }

  /**
   * Un-subscribes the streaming buffer.
   */
  @VisibleForTesting
  protected void unSubscribeBuffer() {
    if ( this.subject != null ) {
      this.subject.dispose();
      this.subject = null;
    }
  }

  /**
   * Un-subscribes the streaming fallback buffer.
   */
  @VisibleForTesting
  protected void unSubscribeFallbackBuffer() {
    if ( this.fallbackSubject != null ) {
      this.fallbackSubject.dispose();
      this.fallbackSubject = null;
    }
  }

  /**
   * Processes a buffer data window.
   *
   * @return The {@link io.reactivex.Observable} window buffer to process.
   */
  private void processBufferWindow( List<RowMetaAndData> list ) {
    if ( isProcessing.compareAndSet( false, true ) ) {
      if ( this.fallbackSubject != null ) {
        this.fallbackSubject.dispose();
        this.fallbackSubject = this.fallbackBuffer.subscribe( bufferList -> {
          processFallbackWindow( bufferList );
        } );
      }
      setCacheWindow( list );
      isProcessing.compareAndSet( true, false );
    }
  }

  /**
   * Processes a fallback buffer data window.
   *
   * @return The {@link io.reactivex.Observable} fallback window buffer to process.
   */
  private void processFallbackWindow( List<RowMetaAndData> list ) {
    if ( isProcessing.compareAndSet( false, true ) ) {
      if ( this.subject != null ) {
        this.subject.dispose();
        this.subject = this.buffer.subscribe( bufferList -> {
          processBufferWindow( bufferList );
        } );
      }
      setCacheWindow( list );
      isProcessing.compareAndSet( true, false );
    }
  }

  /**
   * Sets the cache window data with the given list.
   *
   * @param list - The {@link List} to set to cache.
   */
  private void setCacheWindow( List<RowMetaAndData> list ) {
    this.cachedWindow.clear();
    this.cachedWindow.addAll( list );
  }
}
