/*
 * This program is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License, version 2.1 as published by the Free Software
 * Foundation.
 *
 * You should have received a copy of the GNU Lesser General Public License along with this
 * program; if not, you can obtain a copy at http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html
 * or from the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Lesser General Public License for more details.
 *
 * Copyright (c) 2009 Pentaho Corporation..  All rights reserved.
 */
package org.pentaho.di.ui.repository.repositoryexplorer.abs;

import java.util.HashMap;
import java.util.Map;

import org.pentaho.di.repository.AbsSecurityAdmin;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositorySecurityProvider;
import org.pentaho.di.repository.UserInfo;
import org.pentaho.di.ui.repository.repositoryexplorer.RepositoryExplorer;
import org.pentaho.di.ui.repository.repositoryexplorer.abs.controller.AbsController;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.SpoonLifecycleListener;
import org.pentaho.di.ui.spoon.SpoonPerspective;
import org.pentaho.di.ui.spoon.SpoonPlugin;
import org.pentaho.ui.xul.XulOverlay;
import org.pentaho.ui.xul.impl.DefaultXulOverlay;
import org.pentaho.ui.xul.impl.XulEventHandler;

public class AbsSpoonPlugin implements SpoonPlugin, SpoonLifecycleListener{
  

  public AbsSpoonPlugin() {
    RepositoryExplorer.setSecurityControllerClass(AbsController.class);
  }
  public Map<String, XulEventHandler> getEventHandlers() {
    return null;
  }

  public Map<String, XulOverlay> getOverlays() {
  	HashMap<String, XulOverlay> hash = new HashMap<String, XulOverlay>();
  	
  	XulOverlay overlay = new DefaultXulOverlay("org/pentaho/di/ui/repository/repositoryexplorer/abs/xul/abs-layout-overlay.xul"); //$NON-NLS-1$ 
    hash.put("action-based-security", overlay); //$NON-NLS-1$
    return hash;
  }

  public SpoonLifecycleListener getLifecycleListener() {
    return this;
  }

  public SpoonPerspective getPerspective() {
    return null;
  }
  public void onEvent(SpoonLifeCycleEvent evt) {
    switch(evt) {
      case STARTUP:
        doOnStartup();
        break;
      case SHUTDOWN:
        doOnShutdown();
        break;
    }
  }
  
  private void doOnStartup() {
    Spoon.getInstance().getRegistery().registerSecurityProvider(AbsSecurityAdmin.class);
  }
  private void doOnShutdown() {
    
  }
}
