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
