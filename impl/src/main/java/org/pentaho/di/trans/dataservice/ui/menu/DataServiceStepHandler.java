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

package org.pentaho.di.trans.dataservice.ui.menu;

import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.ui.DataServiceDelegate;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;

public class DataServiceStepHandler extends AbstractXulEventHandler {

  public static Class<?> PKG = DataServiceStepHandler.class;

  private static final String HANDLER_NAME = "dataServiceStepHandler";

  private DataServiceDelegate delegate;

  public DataServiceStepHandler( DataServiceContext context ) {
    delegate = context.getDataServiceDelegate();
  }

  @Override
  public String getName() {
    return HANDLER_NAME;
  }

  public void newDataService() {
    DataServiceMeta dataServiceByStepName =
      delegate.getDataServiceByStepName( getActiveTrans(), getCurrentStep().getName() );
    if ( dataServiceByStepName != null && !dataServiceByStepName.isUserDefined() ) {
      delegate.removeDataService( dataServiceByStepName );
    }
    delegate.createNewDataService( getCurrentStep().getName() );
  }

  public void editDataService() {
    delegate.editDataService( getCurrentDataServiceMeta() );
  }

  public void deleteDataService() {
    delegate.removeDataService( getCurrentDataServiceMeta(), true );
  }

  public void testDataService() {
    delegate.showTestDataServiceDialog( getCurrentDataServiceMeta() );
  }

  public void showDriverDetailsDialog() {
    delegate.showDriverDetailsDialog();
  }

  private DataServiceMeta getCurrentDataServiceMeta() {
    return delegate.getDataServiceByStepName( getActiveTrans(), getCurrentStep().getName() );
  }

  private StepMeta getCurrentStep() {
    return delegate.getSpoon().getActiveTransGraph().getCurrentStep();
  }

  private TransMeta getActiveTrans() {
    return delegate.getSpoon().getActiveTransformation();
  }
}
