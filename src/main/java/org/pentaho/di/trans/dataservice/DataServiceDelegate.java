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

package org.pentaho.di.trans.dataservice;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.optimization.AutoOptimizationService;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownType;
import org.pentaho.di.trans.dataservice.optimization.cache.ServiceCache;
import org.pentaho.di.trans.dataservice.serialization.DataServiceMetaStoreUtil;
import org.pentaho.di.trans.dataservice.ui.DataServiceDialog;
import org.pentaho.di.trans.dataservice.ui.DataServiceTestDialog;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.util.List;

public class DataServiceDelegate {

  private static final Class<?> PKG = DataServiceDelegate.class;
  private static final Log logger = LogFactory.getLog( DataServiceDelegate.class );
  private final DataServiceContext context;
  private final Supplier<Spoon> spoonSupplier;

  private enum DefaultSpoonSupplier implements Supplier<Spoon> {
    INSTANCE;

    @Override public Spoon get() {
      return Spoon.getInstance();
    }
  }

  public static DataServiceDelegate withDefaultSpoonInstance( DataServiceContext context ) {
    return new DataServiceDelegate( context, DefaultSpoonSupplier.INSTANCE );
  }

  public DataServiceDelegate( DataServiceContext context, Spoon spoon ) {
    this( context, Suppliers.ofInstance( spoon ) );
  }

  public DataServiceDelegate( DataServiceContext context, Supplier<Spoon> spoonSupplier ) {
    this.context = context;
    this.spoonSupplier = spoonSupplier;
  }

  public void createNewDataService() {
    createNewDataService( null );
  }

  public void createNewDataService( String stepName ) {
    TransMeta transMeta = getSpoon().getActiveTransformation();
    if ( transMeta.hasChanged() ) {
      showErrors( BaseMessages.getString( PKG, "Messages.Error.Title" ),
        BaseMessages.getString( PKG, "Messages.Error.TransChanged" ) );
      return;
    }
    try {
      DataServiceMeta dataService = new DataServiceMeta( transMeta );
      if ( stepName != null ) {
        dataService.setStepname( stepName );
      }

      for ( PushDownFactory pushDownFactory : context.getPushDownFactories() ) {
        if ( pushDownFactory.getType().equals( ServiceCache.class ) ) {
          PushDownType pushDown = pushDownFactory.createPushDown();

          PushDownOptimizationMeta pushDownOptimizationMeta = new PushDownOptimizationMeta();
          pushDownOptimizationMeta.setName( "Default Cache Optimization" );
          pushDownOptimizationMeta.setStepName( dataService.getStepname() );
          pushDownOptimizationMeta.setType( pushDown );

          dataService.setPushDownOptimizationMeta( Lists.newArrayList( pushDownOptimizationMeta ) );
        }
      }

      DataServiceDialog dialog =
        new DataServiceDialog( getShell(), dataService, transMeta, this );
      dialog.open();
    } catch ( KettleException e ) {
      logger.error( "Unable to create a new data service", e );
    }
  }

  public void editDataService( DataServiceMeta dataService ) {
    TransMeta transMeta = dataService.getServiceTrans();
    if ( transMeta.hasChanged() ) {
      showErrors( BaseMessages.getString( PKG, "Messages.Error.Title" ),
        BaseMessages.getString( PKG, "Messages.Error.TransChanged" ) );
      return;
    }

    try {
      DataServiceDialog dataServiceManagerDialog =
        new DataServiceDialog( getShell(), dataService, transMeta, this );
      dataServiceManagerDialog.open();
    } catch ( KettleException e ) {
      logger.error( "Unable to edit a data service", e );
    }
  }

  public void showErrors( String title, String text ) {
    MessageBox mb = new MessageBox( getShell(), SWT.OK | SWT.ICON_INFORMATION );
    mb.setText( title );
    mb.setMessage( text );
    mb.open();
  }

  public Iterable<DataServiceMeta> getDataServices( TransMeta transMeta ) throws MetaStoreException {
    return getMetaStoreUtil().getDataServices( transMeta );
  }

  public Iterable<DataServiceMeta> getDataServices( Function<Exception, Void> exceptionHandler ) {
    Spoon spoon = getSpoon();
    return getMetaStoreUtil().getDataServices( spoon.getRepository(), spoon.getMetaStore(), exceptionHandler );
  }

  public DataServiceMeta getDataService( String serviceName ) throws MetaStoreException {
    Spoon spoon = getSpoon();
    return getMetaStoreUtil().getDataService( serviceName, spoon.getRepository(), spoon.getMetaStore() );
  }

  public DataServiceMeta getDataService( String serviceName, TransMeta serviceTrans ) throws MetaStoreException {
    return getMetaStoreUtil().getDataService( serviceName, serviceTrans );
  }

  public DataServiceMeta getDataServiceByStepName( TransMeta transMeta, String stepName ) throws MetaStoreException {
    return getMetaStoreUtil().getDataServiceByStepName( transMeta, stepName );
  }

  public List<String> getDataServiceNames() throws MetaStoreException {
    return getMetaStoreUtil().getDataServiceNames( getSpoon().getMetaStore() );
  }

  public void removeDataService( TransMeta transMeta, DataServiceMeta dataService, boolean prompt ) {
    boolean shouldDelete = true;
    if ( prompt ) {
      MessageBox messageBox = new MessageBox( getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION );
      messageBox.setText( BaseMessages.getString( PKG, "Messages.DeleteMessageBox.Title" ) );
      messageBox.setMessage( BaseMessages.getString( PKG, "Messages.DeleteMessageBox.Message" ) );
      int answerIndex = messageBox.open();
      if ( answerIndex != SWT.YES ) {
        shouldDelete = false;
      }
    }
    if ( shouldDelete ) {
      try {
        if ( dataService != null ) {
          getMetaStoreUtil().removeDataService( getSpoon().getMetaStore(), dataService );
          getSpoon().refreshTree();
        }
      } catch ( MetaStoreException e ) {
        logger.error( "Unable to remove a data service", e );
      }
    }
  }

  public void removeDataService( DataServiceMeta dataServiceMeta ) {
    removeDataService( dataServiceMeta.getServiceTrans(), dataServiceMeta, true );
  }

  public void testDataService( DataServiceMeta dataService ) {
    try {
      TransMeta transMeta = dataService.getServiceTrans();
      new DataServiceTestDialog( getSpoon().getShell(), dataService, transMeta ).open();
    } catch ( KettleException e ) {
      logger.error( "Unable to create test data service dialog", e );
    }
  }

  public Spoon getSpoon() {
    return spoonSupplier.get();
  }

  public Shell getShell() {
    return getSpoon().getShell();
  }

  public DataServiceMetaStoreUtil getMetaStoreUtil() {
    return context.getMetaStoreUtil();
  }

  public List<PushDownFactory> getPushDownFactories() {
    return context.getPushDownFactories();
  }

  public List<AutoOptimizationService> getAutoOptimizationServices() {
    return context.getAutoOptimizationServices();
  }

}
