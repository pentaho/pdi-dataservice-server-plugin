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

package org.pentaho.di.trans.dataservice.ui.controller;

import org.junit.Test;
import org.pentaho.ui.xul.swt.tags.SwtDialog;
import org.springframework.util.Assert;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class DataServiceRemapNoStepsDialogControllerTest {
  @Test
  public void testController() {
    SwtDialog dialog = mock( SwtDialog.class );

    DataServiceRemapNoStepsDialogController controller = spy( new DataServiceRemapNoStepsDialogController() );
    Assert.hasText( controller.getName() );
    doReturn( dialog ).when( controller ).getElementById( anyString() );
    controller.close();
    verify( dialog ).dispose();
  }
}
