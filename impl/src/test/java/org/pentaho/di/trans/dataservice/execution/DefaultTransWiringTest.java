/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
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
