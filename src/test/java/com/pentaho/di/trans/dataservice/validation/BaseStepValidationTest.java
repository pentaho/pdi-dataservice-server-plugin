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

package com.pentaho.di.trans.dataservice.validation;

import com.pentaho.di.trans.dataservice.DataServiceMeta;
import com.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import com.pentaho.di.trans.dataservice.optimization.paramgen.ParameterGeneration;
import org.junit.Before;
import org.mockito.Mock;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.CheckStepsExtension;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.api.IMetaStore;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

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
    initMocks( this );
    stepMetas[ 0 ] = stepMeta;
    space = new Variables();
    checkStepsExtension = new CheckStepsExtension(
        remarks, space, transMeta, stepMetas, repository, metaStore );
    pushDownOptMetas = new ArrayList<PushDownOptimizationMeta>();
    when( dataServiceMeta.getPushDownOptimizationMeta() )
        .thenReturn( pushDownOptMetas );
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
