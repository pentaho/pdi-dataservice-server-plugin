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
package org.pentaho.di.trans.dataservice.optimization.cache;

import org.junit.Test;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.step.StepInterface;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BackgroundCacheTest {

  @Test
  public void testActivateChecksRunningTrans() throws Exception {
    final ServiceCache serviceCache = mock( ServiceCache.class );
    final BackgroundCache backgroundCache = new BackgroundCache( serviceCache );
    final DataServiceExecutor executor = mock( DataServiceExecutor.class );
    final StepInterface stepInterface = mock( StepInterface.class );
    when( serviceCache.activate( executor, stepInterface ) ).thenReturn( false, true );
    when( executor.getServiceName() ).thenReturn( "blah" );
    final Trans serviceTrans = mock( Trans.class );
    when( executor.getServiceTrans() ).thenReturn( serviceTrans );
    assertFalse( backgroundCache.activate( executor, stepInterface ) );

    when( serviceTrans.isRunning() ).thenReturn( true, true, false );
    assertTrue( backgroundCache.activate( executor, stepInterface ) );
  }
}