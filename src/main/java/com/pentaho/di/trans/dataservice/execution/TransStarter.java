package com.pentaho.di.trans.dataservice.execution;

import com.google.common.base.Throwables;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.Trans;

/**
 * @author nhudak
 */
public class TransStarter implements Runnable {
  private final Trans trans;

  public TransStarter( Trans trans ) {
    this.trans = trans;
  }

  public Trans getTrans() {
    return trans;
  }

  @Override public void run() {
    try {
      trans.startThreads();
    } catch ( KettleException e ) {
      throw Throwables.propagate( e );
    }
  }
}
