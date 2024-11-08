/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
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

import org.pentaho.di.core.annotations.LifecyclePlugin;
import org.pentaho.di.core.lifecycle.LifeEventHandler;
import org.pentaho.di.core.lifecycle.LifecycleException;
import org.pentaho.di.core.lifecycle.LifecycleListener;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.ui.DataServiceDelegate;
import org.pentaho.di.ui.spoon.Spoon;

/**
 * Created by bmorrise on 7/6/18.
 */
@LifecyclePlugin( id = "DataServiceLifecycleListener" )
public class DataServiceLifecycleListener implements LifecycleListener {

  private DataServiceDelegate dataServiceDelegate;

  public DataServiceLifecycleListener( DataServiceContext context ) {
    dataServiceDelegate = context.getDataServiceDelegate();
  }

  @Override
  public void onStart( LifeEventHandler handler ) throws LifecycleException {
    Spoon.getInstance().getTreeManager().addTreeProvider( Spoon.STRING_CONFIGURATIONS,
            new DataServiceFolderProvider( dataServiceDelegate ) );
  }

  @Override
  public void onExit( LifeEventHandler handler ) throws LifecycleException {

  }
}

