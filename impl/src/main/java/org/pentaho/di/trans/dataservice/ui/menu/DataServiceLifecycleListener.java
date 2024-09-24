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
    Spoon.getInstance().getTreeManager().addTreeProvider( Spoon.STRING_TRANSFORMATIONS,
            new DataServiceFolderProvider( dataServiceDelegate ) );
  }

  @Override
  public void onExit( LifeEventHandler handler ) throws LifecycleException {

  }
}

