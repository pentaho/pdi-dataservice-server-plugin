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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.ConnectException;

import org.pentaho.di.repository.KettleRepositoryLostException;

class UnifiedRepositoryInvocationHandler<T> implements InvocationHandler {
  private T rep;

  // private Repository owner;

  UnifiedRepositoryInvocationHandler( T rep ) {
    this.rep = rep;
  }

  @Override
  public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable {
    try {
      return method.invoke( rep, args );
    } catch ( InvocationTargetException ex ) {
      if ( lookupConnectException( ex ) ) {
        throw new KettleRepositoryLostException( ex.getCause() );
      }

      throw ex.getCause();
    }
  }

  private boolean lookupConnectException( Throwable root ) {
    while ( root != null ) {
      if ( root instanceof ConnectException ) {
        return true;
      } else {
        root = root.getCause();
      }
    }

    return false;
  }

  @SuppressWarnings( "unchecked" )
  public static <T> T forObject( T o, Class<T> clazz ) {
    return (T) Proxy.newProxyInstance( o.getClass().getClassLoader(), new Class<?>[] { clazz },
        new UnifiedRepositoryInvocationHandler<T>( o ) );
  }

}
