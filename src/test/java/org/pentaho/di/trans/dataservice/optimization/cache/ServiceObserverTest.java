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

package org.pentaho.di.trans.dataservice.optimization.cache;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.execution.DefaultTransWiring;
import org.pentaho.di.trans.step.StepInterface;

import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

/**
 * Created by bmorrise on 11/20/15.
 */
@RunWith( MockitoJUnitRunner.class )
public class ServiceObserverTest {

  private static final String STEP_NAME = "Step Name";

  @Mock
  DataServiceExecutor executor;

  @Mock
  StepInterface serviceStep;

  @Mock
  Trans serviceTrans;

  @Mock
  DataServiceMeta dataServiceMeta;

  @Mock
  DefaultTransWiring defaultTransWiring;

  @Mock
  RowMetaInterface rowMetaInterface;

  private ServiceObserver serviceObserver;

  @Before
  public void setUp() {
    serviceObserver = new ServiceObserver( executor );
    defaultTransWiring = new DefaultTransWiring( executor );
    when( executor.getServiceTrans() ).thenReturn( serviceTrans );
    when( serviceTrans.findRunThread( STEP_NAME ) ).thenReturn( serviceStep );
    when( executor.getService() ).thenReturn( dataServiceMeta );
    when( dataServiceMeta.getStepname() ).thenReturn( STEP_NAME );
  }

  @Test
  public void testInstall() {
    ListMultimap<DataServiceExecutor.ExecutionPoint, Runnable>
        listenerMap =
        MultimapBuilder.enumKeys( DataServiceExecutor.ExecutionPoint.class ).linkedListValues().build();

    listenerMap.put( DataServiceExecutor.ExecutionPoint.READY, defaultTransWiring );

    when( executor.getListenerMap() ).thenReturn( listenerMap );

    assertThat( serviceObserver.install(), notNullValue() );
  }

  @Test
  public void testRun() throws KettleException {
    serviceObserver.run();
  }

}
