/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2024 by Hitachi Vantara : http://www.pentaho.com
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
