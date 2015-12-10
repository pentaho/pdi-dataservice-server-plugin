/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.execution;

import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.step.RowAdapter;

import java.util.concurrent.TimeUnit;

/**
 * Created by bryan on 12/10/15.
 */
class DefaultTransWiringRowAdapter extends RowAdapter {
  public static final String PASSING_ALONG_ROW = "Passing along row: ";
  public static final String ROW_BUFFER_IS_FULL_TRYING_AGAIN = "Row buffer is full, trying again";
  private final Trans serviceTrans;
  private final Trans genTrans;
  private final RowProducer rowProducer;

  public DefaultTransWiringRowAdapter( Trans serviceTrans, Trans genTrans, RowProducer rowProducer ) {
    this.serviceTrans = serviceTrans;
    this.genTrans = genTrans;
    this.rowProducer = rowProducer;
  }

  @Override
  public void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
    // Simply pass along the row to the other transformation (to the Injector step)
    //
    LogChannelInterface log = serviceTrans.getLogChannel();
    try {
      if ( log.isRowLevel() ) {
        log.logRowlevel( PASSING_ALONG_ROW + rowMeta.getString( row ) );
      }
    } catch ( KettleValueException e ) {
      // Ignore errors
    }

    try {
      Object[] rowData = rowMeta.cloneRow( row );
      while ( !rowProducer.putRowWait( rowMeta, rowData, 1, TimeUnit.SECONDS ) && genTrans.isRunning() ) {
        // Row queue was full, try again
        if ( log.isRowLevel() ) {
          log.logRowlevel( ROW_BUFFER_IS_FULL_TRYING_AGAIN );
        }
      }
    } catch ( KettleValueException e ) {
      throw new KettleStepException( e );
    }
  }
}
