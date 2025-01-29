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

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.pentaho.di.core.logging.HasLogChannelInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogParentProvidedInterface;
import org.pentaho.di.ui.spoon.trans.LogBrowser;

public class DataServiceTestLogBrowser {

  private final Composite parentComposite;
  private  StyledText logText;

  public DataServiceTestLogBrowser( Composite parentComposite ) {
    this.parentComposite = parentComposite;
  }

  public void attachToLogBrowser( final LogChannelInterface logChannel ) {
    initStyledText( parentComposite );
    LogBrowser logBrowser = new LogBrowser( logText, new LogParentProvidedInterface() {
      @Override
      public HasLogChannelInterface getLogChannelProvider() {
        return new HasLogChannelInterface() {
          @Override
          public LogChannelInterface getLogChannel() {
            return logChannel;
          }
        };
      }
    } );
    logBrowser.installLogSniffer();
  }

  public void dispose() {
    if ( logText != null ) {
      logText.dispose();
    }
  }

  private void initStyledText( Composite composite ) {
    dispose();
    composite.setLayout( new FormLayout() );
    logText = new StyledText( composite, SWT.H_SCROLL | SWT.V_SCROLL );
    FormData formData = new FormData();
    formData.left = new FormAttachment( 0, 0 );
    formData.right = new FormAttachment( 100, 0 );
    formData.top = new FormAttachment( 0, 0 );
    formData.bottom = new FormAttachment( 100, 0 );
    logText.setLayoutData( formData );
    composite.layout();
  }

}
