package com.pentaho.di.trans.dataservice.validation;

import org.junit.Test;
import org.pentaho.di.trans.CheckStepsExtension;
import org.pentaho.di.trans.step.StepMeta;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class StepValidationExtensionPointPluginTest extends BaseStepValidationTest {


  @Test
  public void testCallExtensionPointInvalidObjType() throws Exception {
    StepValidationExtensionPointPlugin extensionPointPlugin =
        new StepValidationExtensionPointPlugin();

    extensionPointPlugin.callExtensionPoint( log, "FooBar" );
    //make sure we logged an error
    verify( log ).logError( any( String.class ) );
  }

  @Test
  public void testCallExtensionPointWrongNumSteps() throws Exception {
    StepValidationExtensionPointPlugin extensionPointPlugin =
        new StepValidationExtensionPointPlugin();

    CheckStepsExtension spiedCheckStepExtension = spy( checkStepsExtension );
    when( spiedCheckStepExtension.getStepMetas() )
        .thenReturn( new StepMeta[ 5 ] );

    extensionPointPlugin.callExtensionPoint( log, spiedCheckStepExtension );
    //make sure we logged an error
    verify( log ).logError( any( String.class ) );
  }

  @Test
  public void testCallExtensionPointDataServiceLoadIssue() throws Exception {
    StepValidationExtensionPointPlugin extensionPointPlugin =
        new StepValidationExtensionPointPlugin();

    CheckStepsExtension spiedCheckStepExtension = spy( checkStepsExtension );
    when( spiedCheckStepExtension.getMetaStore() )
        .thenReturn( null );

    extensionPointPlugin.callExtensionPoint( log, spiedCheckStepExtension );
    //make sure we logged a message
    verify( log ).logBasic( any( String.class ) );
  }

}
