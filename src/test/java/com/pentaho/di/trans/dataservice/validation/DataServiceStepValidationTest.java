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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.pentaho.di.trans.dataservice.optimization.paramgen.ParameterGenerationServiceProvider;
import com.pentaho.metaverse.api.ILineageClient;
import com.pentaho.metaverse.client.StepField;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.platform.api.metaverse.MetaverseException;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class DataServiceStepValidationTest extends BaseStepValidationTest {

  @Mock  private ILineageClient lineageClient;

  @Mock  private ParameterGenerationServiceProvider serviceProvider;

  @InjectMocks  private DataServiceStepValidation dataServiceStepValidation;

  private final String[] FIELD_NAMES = new String[]{ "foo", "bar", "baz" };

  private static final String SERVICE_STEPNAME = "TEST_SVC_STEPNAME";

  @Override
  void init() throws KettleStepException {
    super.init();
    TransMeta testTrans = checkStepsExtension.getTransMeta();

    when( testTrans.getStepFields( SERVICE_STEPNAME ) )
        .thenReturn( mock( RowMetaInterface.class ) );
    when( testTrans.getStepFields( SERVICE_STEPNAME ).getFieldNames() )
        .thenReturn( FIELD_NAMES );
  }

  @Test
  public void unsupportedFieldsAddToRemarks() throws MetaverseException {
    setupServiceStepNameMatches();
    checkStepWithMockedOriginSteps();

    ArgumentCaptor<CheckResultInterface> argumentCaptor = ArgumentCaptor.forClass( CheckResultInterface.class );
    verify( remarks, times( 1 ) ).add( argumentCaptor.capture() );
    for ( String field : FIELD_NAMES ) {
      assertThat( "Expected unsupported field to be in validation messages",
          argumentCaptor.getValue().getText(),
          containsString( field ) );
    }
  }

  @Test
  public void allSupportedFieldsLeaveRemarksUntouched() throws MetaverseException {
    setupServiceStepNameMatches();
    when( serviceProvider.supportsStep( any( StepMeta.class ) ) )
        .thenReturn( true );
    checkStepWithMockedOriginSteps();

    verify( remarks, never() ).add( any( CheckResultInterface.class ) );
  }

  @Test
  public void verifyNonDataServiceStepsIgnored() throws MetaverseException {
    when( dataServiceMeta.getStepname() ).thenReturn( SERVICE_STEPNAME );
    when( stepMeta.getName() ).thenReturn( "NOT " + SERVICE_STEPNAME );
    checkStepWithMockedOriginSteps();

    verify( remarks, never() ).add( any( CheckResultInterface.class ) );
  }

  private void setupServiceStepNameMatches() {
    when( dataServiceMeta.getStepname() ).thenReturn( SERVICE_STEPNAME );
    when( stepMeta.getName() ).thenReturn( SERVICE_STEPNAME );
  }

  private void checkStepWithMockedOriginSteps() throws MetaverseException {
    mockOriginSteps( SERVICE_STEPNAME, Arrays.asList( FIELD_NAMES ) );
    dataServiceStepValidation.checkStep( checkStepsExtension, dataServiceMeta, log );
  }

  private void mockOriginSteps(
      String expectedStepName, Collection<String> fieldNames
  ) throws MetaverseException {
    Map<String, Set<StepField>> stepFieldMap = Maps.newHashMap();
    int i = 0;
    for ( String fieldName : fieldNames ) {
      stepFieldMap
          .put( fieldName,
              ImmutableSet.of(
                  new StepField( "originStepName" + i, "originFieldName" + i++ ) ) );
    }
    when( lineageClient.getOriginSteps(
        eq( checkStepsExtension.getTransMeta() ), eq( expectedStepName ), any( Collection.class ) ) )
        .thenReturn( stepFieldMap );
  }

}


