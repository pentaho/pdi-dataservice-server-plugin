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

package org.pentaho.di.trans.dataservice.optimization;

import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.step.StepInterface;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PushDownOptimizationMetaTest {

  private PushDownOptimizationMeta pushDownOptimizationMeta;
  private DataServiceExecutor executor;
  private Trans trans;
  private PushDownType pushDownType;
  private SQL sql;
  public static final String STEP_NAME = "Optimized Step";

  @Before
  public void setUp() throws Exception {
    pushDownOptimizationMeta = new PushDownOptimizationMeta();
    executor = mock( DataServiceExecutor.class );
    trans = mock( Trans.class );
    pushDownType = mock( PushDownType.class );
    sql = mock( SQL.class );

    pushDownOptimizationMeta.setStepName( STEP_NAME );
    pushDownOptimizationMeta.setType( pushDownType );
    when( executor.getServiceTrans() ).thenReturn( trans );
    when( executor.getSql() ).thenReturn( sql );
  }

  @Test
  public void testActivate() throws Exception {
    assertThat( pushDownOptimizationMeta.activate( executor ), is( false ) );

    StepInterface stepInterface = mock( StepInterface.class );
    when( trans.findRunThread( STEP_NAME ) ).thenReturn( stepInterface );

    when( pushDownType.activate( executor, stepInterface ) ).thenReturn( true, false );
    assertThat( pushDownOptimizationMeta.activate( executor ), is( true ) );
    assertThat( pushDownOptimizationMeta.activate( executor ), is( false ) );
  }
}
