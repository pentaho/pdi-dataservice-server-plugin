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
