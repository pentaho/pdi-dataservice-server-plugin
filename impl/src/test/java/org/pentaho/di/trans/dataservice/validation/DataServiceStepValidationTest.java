/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package org.pentaho.di.trans.dataservice.validation;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.optimization.paramgen.ParameterGenerationFactory;
import org.pentaho.metaverse.api.ILineageClient;
import org.pentaho.metaverse.api.MetaverseException;
import org.pentaho.metaverse.api.StepField;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DataServiceStepValidationTest extends BaseStepValidationTest {

  @Mock  private ILineageClient lineageClient;

  @Mock  private ParameterGenerationFactory serviceProvider;

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
    when( serviceProvider.supportsStep( any() ) )
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


