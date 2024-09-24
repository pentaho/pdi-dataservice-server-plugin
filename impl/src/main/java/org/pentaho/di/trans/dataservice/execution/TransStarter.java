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

package org.pentaho.di.trans.dataservice.execution;

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
