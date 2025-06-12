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


package org.pentaho.di.trans.dataservice.ui;

import com.google.common.collect.Lists;
import org.eclipse.swt.widgets.Shell;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.streaming.ui.StreamingOverlay;
import org.pentaho.di.trans.dataservice.ui.controller.DataServiceDialogController;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceModel;
import org.pentaho.di.ui.xul.KettleXulLoader;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.XulLoader;
import org.pentaho.ui.xul.XulRunner;
import org.pentaho.ui.xul.containers.XulTabbox;
import org.pentaho.ui.xul.swt.SwtXulRunner;

import java.util.ArrayList;
import java.util.ResourceBundle;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */
@RunWith( MockitoJUnitRunner.StrictStubs.class)
public class DataServiceDialogTest {

  @Mock XulDomContainer xulDomContainer;
  @Mock DataServiceModel model;
  @Mock DataServiceDialogController controller;
  @Mock TransMeta transMeta;
  @Mock StreamingOverlay streamingOverlay;

  DataServiceDialog dialog;

  @Before
  public void setUp() throws Exception {
    when( controller.getXulDomContainer() ).thenReturn( xulDomContainer );
    dialog = new DataServiceDialog( controller, model );
  }

  @Test
  public void testInitStreaming() throws Exception {
    XulTabbox tabBox = mock( XulTabbox.class, RETURNS_DEEP_STUBS );
    when( controller.getElementById( "optimizationTabs" ) ).thenReturn( tabBox );
    when( tabBox.getTabs().getTabCount() ).thenReturn( 2 );

    // Create first overlay, low priority
    dialog.initStreaming( streamingOverlay );

    verify( streamingOverlay ).apply( dialog );
    verify( tabBox ).setSelectedIndex( 0 );

    reset( tabBox );
    when( tabBox.getTabs().getTabCount() ).thenReturn( 0 );
    dialog.initStreaming( streamingOverlay );
    verify( tabBox, times( 0 ) ).setSelectedIndex( 0 );
  }

  @Test
  public void testInitOptimizations() throws Exception {
    PushDownFactory pushDownFactory;
    ArrayList<PushDownFactory> factories = Lists.newArrayList();

    XulTabbox tabBox = mock( XulTabbox.class, RETURNS_DEEP_STUBS );
    when( controller.getElementById( "optimizationTabs" ) ).thenReturn( tabBox );
    when( tabBox.getTabs().getTabCount() ).thenReturn( 2 );

    // Create first overlay, low priority
    pushDownFactory = mock( PushDownFactory.class );
    DataServiceDialog.OptimizationOverlay overlay1 = mock( DataServiceDialog.OptimizationOverlay.class );
    when( pushDownFactory.createOverlay() ).thenReturn( overlay1 );
    when( overlay1.getPriority() ).thenReturn( 100.0 );
    factories.add( pushDownFactory );

    // Create another overlay, higher priority
    pushDownFactory = mock( PushDownFactory.class );
    DataServiceDialog.OptimizationOverlay overlay0 = mock( DataServiceDialog.OptimizationOverlay.class );
    when( pushDownFactory.createOverlay() ).thenReturn( overlay0 );
    when( overlay0.getPriority() ).thenReturn( 0.0 );
    factories.add( pushDownFactory );

    // Create push down factory without overlay
    pushDownFactory = mock( PushDownFactory.class );
    when( pushDownFactory.createOverlay() ).thenReturn( null );
    factories.add( pushDownFactory );

    dialog.initOptimizations( factories );

    InOrder inOrder = inOrder( overlay0, overlay1 );
    inOrder.verify( overlay0 ).apply( dialog );
    inOrder.verify( overlay1 ).apply( dialog );
    inOrder.verifyNoMoreInteractions();

    verify( tabBox ).setSelectedIndex( 0 );

    reset( tabBox );
    when( tabBox.getTabs().getTabCount() ).thenReturn( 0 );
    dialog.initOptimizations( factories );
    verify( tabBox, times( 0 ) ).setSelectedIndex( 0 );
  }

  @Test
  public void testDelegates() throws Exception {
    assertThat( dialog.getController(), equalTo( controller ) );
    assertThat( dialog.getModel(), equalTo( model ) );
    assertThat( dialog.getXulDomContainer(), equalTo( xulDomContainer ) );

    dialog.open();
    verify( controller ).open();

    dialog.close();
    verify( controller ).close();
  }

  @Test
  public void testLoadXul() throws Exception {
    Shell shell = mock( Shell.class );
    XulLoader xulLoader = mock( XulLoader.class );
    XulRunner xulRunner = mock( XulRunner.class );
    XulDomContainer xulDomContainer = mock( XulDomContainer.class );
    final ResourceBundle resourceBundle = mock( ResourceBundle.class );

    DataServiceDialog dialog = new DataServiceDialog( controller, model ) {
      @Override protected ResourceBundle createResourceBundle( Class<?> packageClass ) {
        assertThat( packageClass, equalTo( (Class) DataServiceDialog.class ) );
        assertThat( super.createResourceBundle( packageClass ), isA( ResourceBundle.class ) );
        return resourceBundle;
      }
    };
    when( xulLoader.loadXul( anyString(), same( resourceBundle ) ) ).thenReturn( xulDomContainer );

    dialog.loadXul( shell, xulLoader, xulRunner );

    verify( xulLoader ).setOuterContext( shell );
    verify( xulLoader ).registerClassLoader( any( ClassLoader.class ) );
    verify( xulDomContainer ).addEventHandler( controller );
    verify( xulRunner ).addContainer( xulDomContainer );
    verify( xulRunner ).initialize();
  }

  @Test
  public void testApplyOverlay() throws Exception {
    final DataServiceDialog.OptimizationOverlay overlay = mock( DataServiceDialog.OptimizationOverlay.class );
    final ResourceBundle resourceBundle = mock( ResourceBundle.class );
    String xulOverlay = "/path/to/pushdown-overlay.xul";

    dialog = new DataServiceDialog( controller, model ) {
      // Intercept resource bundle creation
      @Override protected ResourceBundle createResourceBundle( Class<?> packageClass ) {
        assertThat( packageClass, equalTo( (Class) overlay.getClass() ) );
        assertThat( super.createResourceBundle( packageClass ), isA( ResourceBundle.class ) );
        return resourceBundle;
      }
    };

    dialog.applyOverlay( overlay, xulOverlay );

    verify( xulDomContainer ).loadOverlay( xulOverlay, resourceBundle );
  }

  @Test( expected = KettleException.class )
  public void testApplyOverlayException() throws Exception {
    final DataServiceDialog.OptimizationOverlay overlay = mock( DataServiceDialog.OptimizationOverlay.class );
    final ResourceBundle resourceBundle = mock( ResourceBundle.class );
    String xulOverlay = "/path/to/pushdown-overlay.xul";

    dialog = new DataServiceDialog( controller, model ) {
      // Intercept resource bundle creation
      @Override protected ResourceBundle createResourceBundle( Class<?> packageClass ) {
        assertThat( packageClass, equalTo( (Class) overlay.getClass() ) );
        assertThat( super.createResourceBundle( packageClass ), isA( ResourceBundle.class ) );
        return resourceBundle;
      }
    };

    doThrow(  new XulException() ).when( xulDomContainer ).loadOverlay( xulOverlay, resourceBundle );

    dialog.applyOverlay( overlay, xulOverlay );
  }

  @Test
  public void testBuilder() throws Exception {
    final DataServiceDelegate mockDelegate = mock( DataServiceDelegate.class );
    final DataServiceDialog dialog = mock( DataServiceDialog.class );
    when( dialog.getController() ).thenReturn( controller );

    // Intercept actual creation so we can inject our mock
    DataServiceDialog.Builder builder = new DataServiceDialog.Builder( model ) {
      @Override protected DataServiceDialog createDialog( DataServiceDelegate delegate ) {
        assertThat( delegate, sameInstance( mockDelegate ) );
        assertThat( super.createDialog( delegate ), isA( DataServiceDialog.class ) );
        return dialog;
      }
    };

    builder.serviceStep( "step" );
    verify( model ).setServiceStep( "step" );

    builder.serviceStep( "" );
    builder.serviceStep( null );
    verify( model, times( 2 ) ).setServiceStep( "" );

    DataServiceMeta dataService = mock( DataServiceMeta.class );
    when( dataService.getName() ).thenReturn( "Service" );
    when( dataService.getStepname() ).thenReturn( "OUTPUT" );
    ArrayList<PushDownOptimizationMeta> optimizations = Lists.newArrayList( mock( PushDownOptimizationMeta.class ) );
    when( dataService.getPushDownOptimizationMeta() ).thenReturn( optimizations );

    builder.edit( dataService );

    verify( model ).setServiceName( "Service" );
    verify( model ).setServiceStep( "OUTPUT" );
    verify( model ).setPushDownOptimizations( optimizations );

    Shell shell = mock( Shell.class );
    ArrayList<PushDownFactory> factories = Lists.newArrayList( mock( PushDownFactory.class ) );
    when( mockDelegate.getShell() ).thenReturn( shell );
    when( mockDelegate.getPushDownFactories() ).thenReturn( factories );

    assertThat( builder.build( mockDelegate ), sameInstance( dialog ) );
    verify( controller ).setDataService( dataService );
    verify( dialog ).loadXul( same( shell ), any( KettleXulLoader.class ), any( SwtXulRunner.class ) );
    verify( dialog ).initOptimizations( factories );

    Throwable xulException = new XulException();
    when( dialog.loadXul( same( shell ), any( KettleXulLoader.class ), any( SwtXulRunner.class ) ) )
      .thenThrow( xulException );

    try {
      builder.build( mockDelegate );
      fail( "Expected exception was not thrown" );
    } catch ( KettleException e ) {
      assertThat( e.getCause(), equalTo( xulException ) );
    }
  }

  @Test
  public void testBuilderTransMeta() throws Exception {
    final DataServiceDelegate mockDelegate = mock( DataServiceDelegate.class );
    final DataServiceDialog dialog = mock( DataServiceDialog.class );
    when( dialog.getController() ).thenReturn( controller );

    // Intercept actual creation so we can inject our mock
    DataServiceDialog.Builder builder = new DataServiceDialog.Builder( transMeta ) {
      @Override protected DataServiceDialog createDialog( DataServiceDelegate delegate ) {
        assertThat( delegate, sameInstance( mockDelegate ) );
        assertThat( super.createDialog( delegate ), isA( DataServiceDialog.class ) );
        return dialog;
      }
    };

    builder.serviceStep( "step" );

    builder.serviceStep( "" );
    builder.serviceStep( null );

    DataServiceMeta dataService = mock( DataServiceMeta.class );
    when( dataService.getName() ).thenReturn( "Service" );
    when( dataService.getStepname() ).thenReturn( "OUTPUT" );
    ArrayList<PushDownOptimizationMeta> optimizations = Lists.newArrayList( mock( PushDownOptimizationMeta.class ) );
    when( dataService.getPushDownOptimizationMeta() ).thenReturn( optimizations );

    builder.edit( dataService );

    Shell shell = mock( Shell.class );
    ArrayList<PushDownFactory> factories = Lists.newArrayList( mock( PushDownFactory.class ) );
    when( mockDelegate.getShell() ).thenReturn( shell );
    when( mockDelegate.getPushDownFactories() ).thenReturn( factories );

    assertThat( builder.build( mockDelegate ), sameInstance( dialog ) );
    verify( controller ).setDataService( dataService );
    verify( dialog ).loadXul( same( shell ), any( KettleXulLoader.class ), any( SwtXulRunner.class ) );
    verify( dialog ).initOptimizations( factories );

    Throwable xulException = new XulException();
    when( dialog.loadXul( same( shell ), any( KettleXulLoader.class ), any( SwtXulRunner.class ) ) )
      .thenThrow( xulException );

    try {
      builder.build( mockDelegate );
      fail( "Expected exception was not thrown" );
    } catch ( KettleException e ) {
      assertThat( e.getCause(), equalTo( xulException ) );
    }
  }
}
