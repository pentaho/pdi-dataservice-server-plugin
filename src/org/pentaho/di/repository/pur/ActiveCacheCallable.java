/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2014 Pentaho Corporation (Pentaho). All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Pentaho and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Pentaho and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Pentaho is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Pentaho,
 * explicitly covering such access.
 */

package org.pentaho.di.repository.pur;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public class ActiveCacheCallable<Key, Value> implements
    Callable<ActiveCacheResult<Value>> {
  private final Key key;
  private final ActiveCacheLoader<Key, Value> loader;
  private final Object syncObject;
  private final Map<Key, ActiveCacheResult<Value>> valueMap;
  private final Map<Key, Future<ActiveCacheResult<Value>>> loadingMap;

  public ActiveCacheCallable( Object syncObject, Map<Key, ActiveCacheResult<Value>> valueMap, Map<Key, Future<ActiveCacheResult<Value>>> loadingMap, Key key, ActiveCacheLoader<Key, Value> loader ) {
    this.syncObject = syncObject;
    this.valueMap = valueMap;
    this.loadingMap = loadingMap;
    this.key = key;
    this.loader = loader;
  }

  @Override
  public ActiveCacheResult<Value> call() throws Exception {
    ActiveCacheResult<Value> result = null;
    try {
      result = new ActiveCacheResult<Value>( loader.load( key ), null );
    } catch ( Exception throwable ) {
      result = new ActiveCacheResult<Value>( null, throwable );
    } finally {
      synchronized ( syncObject ) {
        loadingMap.remove( key );
        // Only cache successful calls
        if ( result.getException() == null ) {
          valueMap.put( key, result );
        }
      }
    }
    return result;
  }
}
