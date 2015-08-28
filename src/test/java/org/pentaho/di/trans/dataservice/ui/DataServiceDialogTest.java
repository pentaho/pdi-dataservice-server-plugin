package org.pentaho.di.trans.dataservice.ui;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.pentaho.di.trans.dataservice.ui.controller.DataServiceDialogController;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceModel;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.containers.XulTabbox;

import java.util.ArrayList;
import java.util.ResourceBundle;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */
@RunWith( MockitoJUnitRunner.class )
public class DataServiceDialogTest {

  @Mock XulDomContainer xulDomContainer;
  @Mock DataServiceModel model;
  @Mock DataServiceDialogController controller;

  DataServiceDialog dialog;

  @Before
  public void setUp() throws Exception {
    when( controller.getXulDomContainer() ).thenReturn( xulDomContainer );
    dialog = new DataServiceDialog( controller, model );
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
  public void testApplyOverlay() throws Exception {
    final DataServiceDialog.OptimizationOverlay overlay = mock( DataServiceDialog.OptimizationOverlay.class );
    final ResourceBundle resourceBundle = mock( ResourceBundle.class );
    String xulOverlay = "/path/to/overlay.xul";

    dialog = new DataServiceDialog( controller, model ) {
      // Intercept resource bundle creation
      @Override protected ResourceBundle createResourceBundle( Class<?> packageClass ) {
        assertThat( packageClass, equalTo( (Class) overlay.getClass() ) );
        return resourceBundle;
      }
    };

    dialog.applyOverlay( overlay, xulOverlay );

    verify( xulDomContainer ).loadOverlay( xulOverlay, resourceBundle );
  }
}
