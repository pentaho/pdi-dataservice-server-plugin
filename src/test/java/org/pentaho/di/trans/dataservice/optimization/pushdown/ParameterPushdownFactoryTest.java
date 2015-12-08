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

package org.pentaho.di.trans.dataservice.optimization.pushdown;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.pushdown.ui.ParameterPushdownController;
import org.pentaho.di.trans.dataservice.ui.DataServiceDialog;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceModel;
import org.pentaho.ui.xul.XulDomContainer;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */

@RunWith( org.mockito.runners.MockitoJUnitRunner.class )
public class ParameterPushdownFactoryTest {
  private static final Class<ParameterPushdown> TYPE = ParameterPushdown.class;

  @InjectMocks ParameterPushdownFactory factory;

  @Captor ArgumentCaptor<ParameterPushdownController> ctrlCaptor;
  @Captor ArgumentCaptor<PushDownOptimizationMeta> metaCaptor;

  @Mock DataServiceDialog dialog;
  @Mock DataServiceModel dialogModel;
  @Mock XulDomContainer container;

  @Test
  public void testCreatePushDown() throws Exception {
    assertThat( factory.getName(), is( ParameterPushdown.NAME ) );
    assertThat( factory.getType(), is( TYPE ) );
    assertThat( factory.createPushDown(), instanceOf( TYPE ) );
  }

  @Test
  public void testCreateOverlay() throws Exception {
    DataServiceDialog.OptimizationOverlay overlay = factory.createOverlay();
    assertThat( overlay.getPriority(), greaterThan( 1.0 ) );

    PushDownOptimizationMeta meta = new PushDownOptimizationMeta();
    PushDownOptimizationMeta extra = new PushDownOptimizationMeta();
    meta.setType( factory.createPushDown() );

    when( dialog.applyOverlay( overlay, ParameterPushdownFactory.XUL_SOURCE ) ).thenReturn( container );
    when( dialog.getModel() ).thenReturn( dialogModel );
    when( dialogModel.getPushDownOptimizations( TYPE ) )
      .thenReturn( ImmutableList.<PushDownOptimizationMeta>of() )
      .thenReturn( ImmutableList.of( meta, extra ) );

    overlay.apply( dialog );
    verify( dialogModel ).add( metaCaptor.capture() );

    overlay.apply( dialog );
    verify( dialogModel ).removeAll( ImmutableList.of( extra ) );

    verify( container, times( 2 ) ).addEventHandler( ctrlCaptor.capture() );
    assertThat( getDelegate( 0 ), sameInstance( metaCaptor.getValue().getType() ) );
    assertThat( getDelegate( 1 ), sameInstance( meta.getType() ) );
  }

  public ParameterPushdown getDelegate( int index ) {
    return ctrlCaptor.getAllValues().get( index ).getModel().getDelegate();
  }
}
