/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.ui;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.pentaho.di.trans.dataservice.ui.controller.DataServiceDialogController;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceModel;
import org.pentaho.di.ui.xul.KettleXulLoader;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.XulRunner;
import org.pentaho.ui.xul.containers.XulTabbox;
import org.pentaho.ui.xul.swt.SwtXulLoader;
import org.pentaho.ui.xul.swt.SwtXulRunner;

import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.ResourceBundle;

public class DataServiceDialog {

  private static final String XUL_DIALOG_PATH = "org/pentaho/di/trans/dataservice/ui/xul/dataservice-dialog.xul";
  private final DataServiceDialogController controller;
  private final DataServiceModel model;

  private static final Class<?> PKG = DataServiceDialog.class;
  private static final ResourceBundle RESOURCE_BUNDLE = new ResourceBundle() {
    @Override
    public Enumeration<String> getKeys() {
      return Collections.emptyEnumeration();
    }

    @Override
    protected Object handleGetObject( String key ) {
      return BaseMessages.getString( PKG, key );
    }
  };

  public static DataServiceDialog create( DataServiceDelegate delegate, TransMeta serviceTrans, String stepName )
    throws KettleException {
    return new Builder( serviceTrans ).serviceStep( stepName ).build( delegate );
  }

  public static DataServiceDialog edit( DataServiceDelegate delegate, DataServiceMeta dataService )
    throws KettleException {
    return new Builder( dataService.getServiceTrans() ).edit( dataService ).build( delegate );
  }

  public DataServiceDialog( DataServiceDelegate delegate, DataServiceModel model ) {
    this( new DataServiceDialogController( model, delegate ), model );
  }

  public DataServiceDialog( DataServiceDialogController controller, DataServiceModel model ) {
    this.controller = controller;
    this.model = model;
  }

  protected DataServiceDialog loadXul( Shell shell ) throws KettleException {
    try {
      SwtXulLoader swtLoader = new KettleXulLoader();
      swtLoader.setOuterContext( shell );
      swtLoader.registerClassLoader( DataServiceDialog.class.getClassLoader() );

      XulDomContainer container = swtLoader.loadXul( XUL_DIALOG_PATH, RESOURCE_BUNDLE );
      container.addEventHandler( controller );

      XulRunner runner = new SwtXulRunner();
      runner.addContainer( container );
      runner.initialize();
    } catch ( XulException xulException ) {
      throw new KettleException( "Failed to open the Data Service Dialog ", xulException );
    }

    return this;
  }

  protected DataServiceDialog initOptimizations( List<PushDownFactory> pushDownFactories )
    throws KettleException {
    ImmutableList<OptimizationOverlay> overlays = FluentIterable.from( pushDownFactories )
      .transform( new Function<PushDownFactory, OptimizationOverlay>() {
        @Override public OptimizationOverlay apply( PushDownFactory input ) {
          return input.createOverlay();
        }
      } )
      .filter( Predicates.notNull() )
      .toSortedList( Ordering.natural().onResultOf( new Function<OptimizationOverlay, Comparable>() {
        @Override public Comparable apply( OptimizationOverlay input ) {
          return input.getPriority();
        }
      } ) );
    for ( OptimizationOverlay overlay : overlays ) {
      overlay.apply( this );
    }
    XulTabbox optimizationTabs = controller.getElementById( "optimizationTabs" );
    if ( optimizationTabs.getTabs().getTabCount() > 0 ) {
      optimizationTabs.setSelectedIndex( 0 );
    }

    return this;
  }

  public XulDomContainer getXulDomContainer() {
    return controller.getXulDomContainer();
  }

  /**
   * Apply an overlay to the dialog.
   *
   * @param classLoader ClassLoader to load the xulOverlay from
   * @param xulOverlay  Path to the XUL overlay to load and apply to the DOM container
   * @throws KettleException Error loading XUL, wrapped in a KettleException
   */
  public XulDomContainer applyOverlay( ClassLoader classLoader, String xulOverlay ) throws KettleException {
    XulDomContainer xulDomContainer = getXulDomContainer();
    try {
      xulDomContainer.registerClassLoader( classLoader );
      xulDomContainer.loadOverlay( xulOverlay );
    } catch ( XulException e ) {
      throw new KettleException( e );
    }
    return xulDomContainer;
  }

  public void open() {
    controller.open();
  }

  public void close() {
    controller.close();
  }

  public DataServiceDialogController getController() {
    return controller;
  }

  public DataServiceModel getModel() {
    return model;
  }

  private static class Builder {
    private final DataServiceModel model;
    private DataServiceMeta dataService;

    Builder( TransMeta transMeta ) {
      this.model = new DataServiceModel( transMeta );
    }

    Builder serviceStep( String serviceStep ) {
      model.setServiceStep( Strings.nullToEmpty( serviceStep ) );
      return this;
    }

    Builder edit( DataServiceMeta dataService ) {
      this.dataService = dataService;
      model.setServiceName( dataService.getName() );
      model.setServiceStep( dataService.getStepname() );
      model.setPushDownOptimizations( dataService.getPushDownOptimizationMeta() );
      return this;
    }

    DataServiceDialog build( DataServiceDelegate delegate ) throws KettleException {
      DataServiceDialog dialog = new DataServiceDialog( delegate, model );
      dialog.controller.setDataService( dataService );

      dialog
        .loadXul( delegate.getShell() )
        .initOptimizations( delegate.getPushDownFactories() );

      return dialog;
    }

  }

  public interface OptimizationOverlay {
    double getPriority();

    void apply( DataServiceDialog dialog ) throws KettleException;
  }

}
