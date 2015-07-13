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

import com.google.common.base.Throwables;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.StepAdapter;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;

import java.util.concurrent.TimeUnit;

/**
 * @author nhudak
 */
public class DefaultTransWiring implements Runnable {
  private final DataServiceExecutor dataServiceExecutor;

  public DefaultTransWiring( DataServiceExecutor dataServiceExecutor ) {
    this.dataServiceExecutor = dataServiceExecutor;
  }

  @Override public void run() {
    // This is where we will inject the rows from the service transformation step
    //
    final RowProducer rowProducer;
    final Trans serviceTrans = dataServiceExecutor.getServiceTrans();
    final Trans genTrans = dataServiceExecutor.getGenTrans();

    try {
      rowProducer = dataServiceExecutor.addRowProducer();
    } catch ( KettleException e ) {
      throw Throwables.propagate( e );
    }

    // Now connect the 2 transformations with listeners and injector
    //
    StepInterface serviceStep = serviceTrans.findRunThread( dataServiceExecutor.getService().getStepname() );
    serviceStep.addRowListener( new RowAdapter() {
      @Override
      public void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
        // Simply pass along the row to the other transformation (to the Injector step)
        //
        LogChannelInterface log = serviceTrans.getLogChannel();
        try {
          if ( log.isRowLevel() ) {
            log.logRowlevel( "Passing along row: " + rowMeta.getString( row ) );
          }
        } catch ( KettleValueException e ) {
          // Ignore errors
        }

        while ( !rowProducer.putRowWait( rowMeta, row, 1, TimeUnit.SECONDS ) && genTrans.isRunning() ) {
          // Row queue was full, try again
          if ( log.isRowLevel() ) {
            log.logRowlevel( "Row buffer is full, trying again" );
          }
        }
      }
    } );

    // Let the other transformation know when there are no more rows
    //
    serviceStep.addStepListener( new StepAdapter() {
      @Override public void stepFinished( Trans trans, StepMeta stepMeta, StepInterface step ) {
        rowProducer.finished();
      }
    } );

    dataServiceExecutor.getGenTrans()
      .findRunThread( dataServiceExecutor.getResultStepName() )
      .addStepListener( new StepAdapter() {
        @Override public void stepFinished( Trans trans, StepMeta stepMeta, StepInterface step ) {
          if ( serviceTrans.isRunning() ) {
            trans.getLogChannel().logBasic( "Query finished, stopping service transformation" );
            serviceTrans.stopAll();
          }
        }
      } );
  }

}
