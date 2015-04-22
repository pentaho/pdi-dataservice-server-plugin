package com.pentaho.di.trans.dataservice.validation;

import com.google.common.collect.ImmutableList;
import com.pentaho.di.trans.dataservice.DataServiceMetaStoreUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.pentaho.di.trans.CheckStepsExtension;
import org.pentaho.di.trans.step.StepMeta;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StepValidationExtensionPointPluginTest extends BaseStepValidationTest {

  private CheckStepsExtension spiedCheckStepExtension;
  @Mock private DataServiceMetaStoreUtil metaStoreUtil;
  @Mock private StepValidation stepValidation;
  @InjectMocks private StepValidationExtensionPointPlugin extensionPointPlugin;

  @Before
  public void setUp() throws Exception {
    spiedCheckStepExtension = spy( checkStepsExtension );
    extensionPointPlugin.setStepValidations( ImmutableList.of( stepValidation ) );
    when( metaStoreUtil.fromTransMeta( transMeta, metaStore ) ).thenReturn( dataServiceMeta );
  }

  @Test
  public void testCallStepValidations() throws Exception {
    when( stepValidation.supportsStep( stepMeta, log ) ).thenReturn( true, false );

    extensionPointPlugin.callExtensionPoint( log, spiedCheckStepExtension );
    extensionPointPlugin.callExtensionPoint( log, spiedCheckStepExtension );

    verify( stepValidation ).checkStep( spiedCheckStepExtension, dataServiceMeta, log );
  }

  @Test
  public void testCallExtensionPointInvalidObjType() throws Exception {
    extensionPointPlugin.callExtensionPoint( log, "FooBar" );
    //make sure we logged an error
    verify( log ).logError( any( String.class ) );
  }

  @Test
  public void testCallExtensionPointWrongNumSteps() throws Exception {
    when( spiedCheckStepExtension.getStepMetas() )
        .thenReturn( new StepMeta[ 5 ] );

    extensionPointPlugin.callExtensionPoint( log, spiedCheckStepExtension );
    //make sure we logged an error
    verify( log ).logError( any( String.class ) );
  }

  @Test
  public void testCallExtensionPointDataServiceLoadIssue() throws Exception {
    when( spiedCheckStepExtension.getMetaStore() )
        .thenReturn( null );

    extensionPointPlugin.callExtensionPoint( log, spiedCheckStepExtension );
    //make sure we logged a message
    verify( log ).logBasic( any( String.class ) );
  }

}
