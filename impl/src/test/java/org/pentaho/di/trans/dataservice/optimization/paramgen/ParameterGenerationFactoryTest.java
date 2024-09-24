/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2024 by Hitachi Vantara : http://www.pentaho.com
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
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.trans.dataservice.optimization.SourceTargetFields;
import org.pentaho.di.trans.dataservice.optimization.paramgen.ui.ParameterGenerationController;
import org.pentaho.di.trans.dataservice.optimization.paramgen.ui.ParameterGenerationOverlay;
import org.pentaho.di.trans.dataservice.optimization.paramgen.ui.SourceTargetAdapter;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceModel;
import org.pentaho.di.trans.step.StepMeta;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.StrictStubs.class)
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
    assertThat( provider.createOverlay(), isA( ParameterGenerationOverlay.class ) );
    assertThat( provider.createController( mock( DataServiceModel.class ) ),
      isA( ParameterGenerationController.class ) );
    assertThat( provider.createSourceTargetAdapter( mock( SourceTargetFields.class ) ),
      isA( SourceTargetAdapter.class ) );
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
