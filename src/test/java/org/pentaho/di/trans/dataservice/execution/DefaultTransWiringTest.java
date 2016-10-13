/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;

public class DefaultTransWiringTest {
  private Trans serviceTrans;
  private Trans genTrans;
  private RowProducer rowProducer;
  private DefaultTransWiring defaultTransWiring;
  private DataServiceExecutor dataServiceExecutor;
  private DataServiceMeta dataServiceMeta;

  @Before
  public void setup() throws KettleException {
    serviceTrans = mock( Trans.class );
    genTrans = mock( Trans.class );
    rowProducer = mock( RowProducer.class );
    dataServiceExecutor = mock( DataServiceExecutor.class );
    dataServiceMeta = mock( DataServiceMeta.class );
    when( dataServiceExecutor.addRowProducer() ).thenReturn( rowProducer );
    when( dataServiceExecutor.getServiceTrans() ).thenReturn( serviceTrans );
    when( dataServiceExecutor.getService() ).thenReturn( dataServiceMeta );
    when( dataServiceMeta.getStepname() ).thenReturn( "step" );
    when( dataServiceExecutor.getGenTrans() ).thenReturn( genTrans );
    defaultTransWiring = new DefaultTransWiring( dataServiceExecutor );
  }

  @Test
  public void testInaccessibleServiceStep() throws KettleException {
    when( serviceTrans.findRunThread( "step" ) ).thenReturn( null );
    try {
      defaultTransWiring.run();
    } catch ( Throwable t ) {
      assertEquals( "Service step is not accessible", t.getCause().getMessage().trim() );
    }
  }

}
