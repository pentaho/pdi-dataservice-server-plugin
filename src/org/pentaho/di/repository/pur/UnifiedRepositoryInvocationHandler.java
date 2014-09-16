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
