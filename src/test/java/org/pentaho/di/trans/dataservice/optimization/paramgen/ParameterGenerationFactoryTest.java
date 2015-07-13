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

package org.pentaho.di.trans.dataservice.optimization.paramgen;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.trans.step.StepMeta;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class ParameterGenerationFactoryTest {

  private ParameterGenerationFactory provider;
  @Mock ParameterGenerationServiceFactory serviceFactory;
  @Mock StepMeta stepMeta;

  @Before
  public void setUp() throws Exception {
    provider = new ParameterGenerationFactory( ImmutableList.of( serviceFactory ) );
  }

  @Test
  public void testFactory() throws Exception {
    assertThat( provider.getName(), is( ParameterGeneration.TYPE_NAME ) );
    assertThat( provider.createPushDown(), isA( ParameterGeneration.class ) );
    assertThat( provider.createPushDownOptTypeForm(), isA( ParamGenOptForm.class ) );
  }

  @Test
  public void testServiceProvisioning() throws Exception {
    assertThat( serviceFactory.supportsStep( mock( StepMeta.class ) ), is( false ) );

    when( serviceFactory.supportsStep( stepMeta ) ).thenReturn( false );
    assertThat( serviceFactory.supportsStep( stepMeta ), is( false ) );
    assertThat( provider.getService( stepMeta ), nullValue() );

    ParameterGenerationService service = mock( ParameterGenerationService.class );
    when( serviceFactory.supportsStep( stepMeta ) ).thenReturn( true );
    when( serviceFactory.getService( stepMeta ) ).thenReturn( service );
    assertThat( serviceFactory.supportsStep( stepMeta ), is( true ) );
    assertThat( provider.getService( stepMeta ), is( service ) );
  }
}
