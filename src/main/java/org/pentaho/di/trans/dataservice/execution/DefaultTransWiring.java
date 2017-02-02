/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
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
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.step.StepAdapter;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;

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
    if ( serviceStep == null ) {
      throw Throwables.propagate( new KettleException( "Service step is not accessible" ) );
    }
    serviceStep.addRowListener( new DefaultTransWiringRowAdapter( serviceTrans, genTrans, rowProducer ) );

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
