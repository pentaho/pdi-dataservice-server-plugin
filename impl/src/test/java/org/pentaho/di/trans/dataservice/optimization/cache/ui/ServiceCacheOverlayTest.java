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

package org.pentaho.di.trans.dataservice.optimization.cache.ui;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.trans.dataservice.optimization.cache.ServiceCacheFactory;
import org.pentaho.di.trans.dataservice.ui.DataServiceDialog;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceModel;
import org.pentaho.ui.xul.XulDomContainer;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */
@RunWith( MockitoJUnitRunner.StrictStubs.class)
public class ServiceCacheOverlayTest {

  @Mock DataServiceDialog dialog;
  @Mock ServiceCacheFactory factory;
  @Mock ServiceCacheController controller;
  @Mock DataServiceModel model;
  @Mock XulDomContainer xulDomContainer;
  @InjectMocks ServiceCacheOverlay overlay;

  @Test
  public void testApply() throws Exception {
    when( factory.createController() ).thenReturn( controller );
    when( dialog.applyOverlay( same( overlay ), anyString() ) ).thenReturn( xulDomContainer );
    when( dialog.getModel() ).thenReturn( model );

    overlay.apply( dialog );

    verify( xulDomContainer ).addEventHandler( controller );
    verify( controller ).initBindings( model );
  }
}
