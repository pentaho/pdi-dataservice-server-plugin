/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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
