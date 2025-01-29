/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.di.trans.dataservice.optimization.paramgen.ui;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.eclipse.swt.SWT;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.paramgen.AutoParameterGenerationService;
import org.pentaho.di.trans.dataservice.optimization.paramgen.ParameterGeneration;
import org.pentaho.di.trans.dataservice.optimization.paramgen.ParameterGenerationFactory;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceModel;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.components.XulButton;
import org.pentaho.ui.xul.components.XulMenuList;
import org.pentaho.ui.xul.components.XulMessageBox;
import org.pentaho.ui.xul.components.XulPromptBox;
import org.pentaho.ui.xul.containers.XulListbox;
import org.pentaho.ui.xul.dom.Document;
import org.pentaho.ui.xul.util.XulDialogCallback;

import java.util.List;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */
@RunWith( MockitoJUnitRunner.StrictStubs.class)
public class ParameterGenerationControllerTest {

  @Mock ParameterGenerationFactory factory;
  @Mock ParameterGenerationModel model;
  @Mock XulDomContainer xulDomContainer;
  @Mock Document document;
  @Mock BindingFactory bindingFactory;

  @Mock XulListbox listBox;
  @Mock XulMenuList<String> stepList;
  @Mock XulButton addButton;

  @Mock XulMessageBox messageBox;
  @Mock XulPromptBox promptBox;
  @Captor ArgumentCaptor<XulDialogCallback<String>> dialogCallback;
  ParameterGenerationController controller;

  @Before
  public void setUp() throws Exception {
    // Intercept creation methods so we can inject our mocks
    controller = new ParameterGenerationController( factory, model );
    when( document.createElement( "promptbox" ) ).then( new Answer<XulMessageBox>() {
      @Override public XulMessageBox answer( InvocationOnMock invocation ) throws Throwable {
        return promptBox;
      }
    } );
    when( document.createElement( "messagebox" ) ).then( new Answer<XulMessageBox>() {
      @Override public XulMessageBox answer( InvocationOnMock invocation ) throws Throwable {
        return messageBox;
      }
    } );

    when( xulDomContainer.getDocumentRoot() ).thenReturn( document );
    when( document.getElementById( "param_gen_add" ) ).thenReturn( addButton );
    when( document.getElementById( "param_gen_step" ) ).thenReturn( stepList );

    controller.setXulDomContainer( xulDomContainer );
    controller.setBindingFactory( bindingFactory );
  }

  @Test
  @SuppressWarnings( "unchecked" )
  public void testInitBindings() throws Exception {
    List<String> supportedSteps = ImmutableList.of( "input" );

    controller.initBindings( supportedSteps );

    verify( addButton ).setDisabled( false );
    verify( stepList ).setElements( supportedSteps );

    verify( bindingFactory, atLeastOnce() ).createBinding( same( model ), anyString(), anyString(), anyString() );
    verify( model ).updateParameterMap();
  }

  @Test
  public void testRunAutoGenerate() throws Exception {
    DataServiceModel dataServiceModel = mock( DataServiceModel.class );
    when( model.getDialogModel() ).thenReturn( dataServiceModel );

    AutoParameterGenerationService parameterGenerationService = mock( AutoParameterGenerationService.class );
    when( factory.createAutoOptimizationService() ).thenReturn( parameterGenerationService );

    DataServiceMeta dataServiceMeta = mock( DataServiceMeta.class );
    when( dataServiceModel.getDataService() ).thenReturn( dataServiceMeta );

    List<PushDownOptimizationMeta> generatedOptimizations = ImmutableList.of( mock( PushDownOptimizationMeta.class ) );
    when( parameterGenerationService.apply( dataServiceMeta ) ).thenReturn( generatedOptimizations );

    when( dataServiceModel.addAll( generatedOptimizations ) ).thenReturn( true );

    controller.runAutoGenerate();

    verify( dataServiceModel ).addAll( generatedOptimizations );
    verify( model ).updateParameterMap();
    verify( messageBox ).open();
  }

  @Test
  public void testAddParameter() throws Exception {
    ParameterGeneration parameterGeneration = new ParameterGeneration( factory );

    when( factory.createPushDown() ).thenReturn( parameterGeneration );
    when( model.getParameterMap() ).thenReturn( ImmutableMap.<String, PushDownOptimizationMeta>of() );
    when( stepList.getSelectedItem() ).thenReturn( "Default Step" );

    mockPromptBox( "parameterName", XulDialogCallback.Status.ACCEPT );
    controller.addParameter();

    ArgumentCaptor<PushDownOptimizationMeta> optMetaCaptor = ArgumentCaptor.forClass( PushDownOptimizationMeta.class );
    verify( model ).add( optMetaCaptor.capture() );
    PushDownOptimizationMeta pushDownOptimizationMeta = optMetaCaptor.getValue();

    assertThat( pushDownOptimizationMeta, allOf(
      hasProperty( "type", sameInstance( parameterGeneration ) ),
      hasProperty( "enabled", is( true ) ),
      hasProperty( "stepName", is( "Default Step" ) )
    ) );
    assertThat( parameterGeneration.getParameterName(), equalTo( "parameterName" ) );

    verify( model ).setSelectedParameter( "parameterName" );
  }

  private void mockPromptBox( final String callbackValue, final XulDialogCallback.Status returnCode ) {
    promptBox = mock( XulPromptBox.class );
    when( promptBox.open() ).then( new Answer<Integer>() {
      @Override public Integer answer( InvocationOnMock invocation ) throws Throwable {
        verify( promptBox ).addDialogCallback( dialogCallback.capture() );
        dialogCallback.getValue().onClose( promptBox, returnCode, callbackValue );
        return returnCode.ordinal();
      }
    } );
  }

  @Test
  public void testAddParameterFailure() throws Exception {
    when( factory.createPushDown() ).thenReturn( new ParameterGeneration( factory ) );
    when( model.getParameterMap() ).thenReturn( ImmutableMap.<String, PushDownOptimizationMeta>of() );
    when( stepList.getSelectedItem() ).thenReturn( "Default Step" );

    mockPromptBox( "parameterName", XulDialogCallback.Status.CANCEL );
    controller.addParameter();

    mockPromptBox( "", XulDialogCallback.Status.ACCEPT );
    controller.addParameter();

    mockPromptBox( "parameterName", XulDialogCallback.Status.ACCEPT );
    when( model.getParameterMap() ).thenReturn( ImmutableMap.of(
      "parameterName", mock( PushDownOptimizationMeta.class )
    ) );
    controller.addParameter();

    verify( model, never() ).add( any( PushDownOptimizationMeta.class ) );
    verify( messageBox, times( 2 ) ).setIcon( SWT.ICON_WARNING );
    verify( messageBox, times( 2 ) ).open();
  }

  @Test
  public void testEditParameter() throws Exception {
    PushDownOptimizationMeta meta = new PushDownOptimizationMeta();
    ParameterGeneration parameterGeneration = new ParameterGeneration( factory );

    parameterGeneration.setParameterName( "parameterName" );
    setSelected( meta, parameterGeneration );

    mockPromptBox( "newParameterName", XulDialogCallback.Status.ACCEPT );
    controller.editParameter();

    assertThat( parameterGeneration.getParameterName(), equalTo( "newParameterName" ) );
    verify( model ).setSelectedParameter( "newParameterName" );
    verify( model ).updateParameterMap();
  }

  @Test
  public void testEditParameterFailure() throws Exception {
    PushDownOptimizationMeta meta = new PushDownOptimizationMeta();
    ParameterGeneration parameterGeneration = new ParameterGeneration( factory );

    parameterGeneration.setParameterName( "parameterName" );
    setSelected( meta, parameterGeneration );

    mockPromptBox( "newParameterName", XulDialogCallback.Status.CANCEL );
    controller.editParameter();

    mockPromptBox( "parameterName", XulDialogCallback.Status.ACCEPT );
    controller.editParameter();

    mockPromptBox( "", XulDialogCallback.Status.ACCEPT );
    controller.editParameter();

    mockPromptBox( "newParameterName", XulDialogCallback.Status.ACCEPT );
    when( model.getParameterMap() ).thenReturn( ImmutableMap.of(
      "parameterName", meta,
      "newParameterName", mock( PushDownOptimizationMeta.class )
    ) );
    controller.editParameter();

    assertThat( parameterGeneration.getParameterName(), equalTo( "parameterName" ) );
    verify( model, never() ).updateParameterMap();
    verify( messageBox, times( 2 ) ).setIcon( SWT.ICON_WARNING );
    verify( messageBox, times( 2 ) ).open();
  }

  private void setSelected( PushDownOptimizationMeta meta, ParameterGeneration parameterGeneration ) {
    meta.setType( parameterGeneration );
    when( model.getSelectedOptimization() ).thenReturn( meta );
    when( model.getParameterGeneration() ).thenReturn( parameterGeneration );
    when( model.getParameterMap() ).thenReturn( ImmutableMap.of(
      parameterGeneration.getParameterName(), meta
    ) );
  }

  @Test
  public void testRemoveParameter() throws Exception {
    PushDownOptimizationMeta meta = new PushDownOptimizationMeta();
    ParameterGeneration parameterGeneration = new ParameterGeneration( factory );

    parameterGeneration.setParameterName( "parameterName" );
    setSelected( meta, parameterGeneration );

    when( messageBox.open() ).thenReturn( SWT.NO );
    controller.removeParameter();
    verify( model, never() ).remove( any( PushDownOptimizationMeta.class ) );

    when( messageBox.open() ).thenReturn( SWT.YES );
    controller.removeParameter();
    verify( model ).setSelectedParameter( null );
    verify( model ).remove( meta );

    verify( messageBox, times( 2 ) ).setIcon( SWT.ICON_QUESTION );
  }
}
