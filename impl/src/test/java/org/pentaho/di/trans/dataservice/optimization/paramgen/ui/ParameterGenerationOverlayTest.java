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


package org.pentaho.di.trans.dataservice.optimization.paramgen.ui;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.optimization.paramgen.ParameterGenerationFactory;
import org.pentaho.di.trans.dataservice.ui.DataServiceDialog;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceModel;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.ui.xul.XulDomContainer;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */
@RunWith( MockitoJUnitRunner.StrictStubs.class)
public class ParameterGenerationOverlayTest {

  @Mock ParameterGenerationFactory factory;
  @Mock DataServiceDialog dialog;
  @Mock DataServiceModel model;
  @InjectMocks ParameterGenerationOverlay overlay;

  @Test
  public void testApply() throws Exception {
    ParameterGenerationController controller = mock( ParameterGenerationController.class );
    XulDomContainer xulDomContainer = mock( XulDomContainer.class );
    TransMeta transMeta = mock( TransMeta.class );
    StepMeta supportedStep = new StepMeta( "supportedStep", mock( StepMetaInterface.class ) );

    when( dialog.getModel() ).thenReturn( model );
    when( model.getTransMeta() ).thenReturn( transMeta );
    when( transMeta.getSteps() ).thenReturn( ImmutableList.of( supportedStep, mock( StepMeta.class ) ) );
    when( factory.supportsStep( supportedStep ) ).thenReturn( true );
    when( factory.createController( model ) ).thenReturn( controller );
    when( dialog.applyOverlay( same( overlay ), anyString() ) ).thenReturn( xulDomContainer );

    overlay.apply( dialog );

    verify( xulDomContainer ).addEventHandler( controller );
    verify( controller ).initBindings( ImmutableList.of( "supportedStep" ) );
  }
}
