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

package org.pentaho.di.trans.dataservice.streaming;

import io.reactivex.subjects.PublishSubject;

/**
 * Generic stream.
 * @param <T>
 */
public class StreamList<T> {
  protected final PublishSubject<T> onAdd;

  /**
   * Constructor.
   */
  public StreamList() {
    this.onAdd = PublishSubject.create();
  }

  /**
   * Adds an item to the stream.
   * @param item The item to be added.
   */
  public void add( T item ) {
    onAdd.onNext( item );
  }

  /**
   * Retreives the stream subject where the client can register as a listener to the stream.
   *
   * @return {@link io.reactivex.subjects.PublishSubject} the stream subject.
   */
  public PublishSubject<T> getStream() {
    return onAdd;
  }
}
