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

package org.pentaho.di.trans.dataservice.validation;

import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.CheckStepsExtension;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.paramgen.ParameterGeneration;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.api.IMetaStore;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.StrictStubs.class)
public abstract class BaseStepValidationTest {

  @Mock
  StepMeta stepMeta;

  @Mock
  TransMeta transMeta;

  @Mock
  IMetaStore metaStore;

  @Mock
  Repository repository;

  @Mock
  List<CheckResultInterface> remarks;

  @Mock
  DataServiceMeta dataServiceMeta;

  @Mock
  LogChannelInterface log;

  List<PushDownOptimizationMeta> pushDownOptMetas;

  VariableSpace space;

  CheckStepsExtension checkStepsExtension;

  StepMeta[] stepMetas = new StepMeta[ 1 ];

  @Before
  public void before() throws KettleStepException {
    stepMetas[ 0 ] = stepMeta;
    space = new Variables();
    checkStepsExtension = new CheckStepsExtension(
        remarks, space, transMeta, stepMetas, repository, metaStore );
    pushDownOptMetas = new ArrayList<PushDownOptimizationMeta>();
    lenient().when( dataServiceMeta.getPushDownOptimizationMeta() ).thenReturn( pushDownOptMetas );
    init();
  }

  void init() throws KettleStepException {
    // no-op.  Overriden by child classes to do test setup.
  }

  ParameterGeneration setupMockedParamGen( String paramName, String stepName ) {
    ParameterGeneration pushDownType = mock( ParameterGeneration.class );
    when( pushDownType.getParameterName() )
        .thenReturn( paramName );
    PushDownOptimizationMeta optMeta = new PushDownOptimizationMeta();
    optMeta.setType( pushDownType );
    optMeta.setStepName( stepName );
    pushDownOptMetas.add( optMeta );
    return pushDownType;
  }

}
