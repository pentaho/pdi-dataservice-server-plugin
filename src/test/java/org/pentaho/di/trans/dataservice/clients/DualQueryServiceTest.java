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

package org.pentaho.di.trans.dataservice.clients;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Created by bmorrise on 9/12/16.
 */
public class DualQueryServiceTest {

  private static final String DUAL_TABLE_NAME = "dual";
  private static final String TEST_DUMMY_SQL_QUERY = "SELECT 1";

  private DualQueryService dualQueryService;

  @Before
  public void setup() {
    dualQueryService = new DualQueryService();
  }

  @Test
  public void testWriteDummyRow() throws Exception {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    dualQueryService.prepareQuery( TEST_DUMMY_SQL_QUERY, -1, ImmutableMap.<String, String>of() ).writeTo( byteArrayOutputStream );

    byte[] data = byteArrayOutputStream.toByteArray();
    assertThat( ByteStreams.newDataInput( data ).readUTF(), equalTo( DUAL_TABLE_NAME ) );
    assertThat( data, equalTo( DualQueryService.DATA ) );
  }

}
