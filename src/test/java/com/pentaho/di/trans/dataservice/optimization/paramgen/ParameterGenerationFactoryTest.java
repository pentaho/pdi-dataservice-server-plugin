/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2015 Pentaho Corporation (Pentaho). All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Pentaho and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Pentaho and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Pentaho is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Pentaho,
 * explicitly covering such access.
 */

package com.pentaho.di.trans.dataservice.optimization.paramgen;

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
