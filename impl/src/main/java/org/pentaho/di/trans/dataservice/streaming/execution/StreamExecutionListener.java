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

import io.reactivex.Observable;
import io.reactivex.disposables.Disposable;
import org.pentaho.di.core.RowMetaAndData;

import java.util.ArrayList;
import java.util.List;

/**
 * Class to represents a listener for a service transformation stream.
 */
public class StreamExecutionListener {
  private Disposable subject;
  private Observable<List<RowMetaAndData>> buffer;
  private List<RowMetaAndData> cachedWindow = new ArrayList<RowMetaAndData>();

  /**
   * Constructor.
   * Subscribes a listener to the given window buffer.
   *
   * @param buffer The {@link io.reactivex.Observable} stream window buffer.
   */
  public StreamExecutionListener( final Observable<List<RowMetaAndData>> buffer ) {
    this.buffer = buffer;
    this.subject = this.buffer.subscribe( list -> this.cachedWindow = list );
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
   * Un-subscribes the streaming buffer.
   */
  public void unSubscribe() {
    if ( this.subject != null ) {
      this.subject.dispose();
      this.subject = null;
    }
  }
}
