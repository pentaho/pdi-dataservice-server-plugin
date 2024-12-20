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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.trans.dataservice.BaseTest;
import org.pentaho.di.trans.dataservice.CommandExecutor;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.verify;

/**
 * {@link CommandQueryService} test class
 */
@RunWith( MockitoJUnitRunner.StrictStubs.class)
public class CommandQueryServiceTest extends BaseTest {
  private static final String TEST_NON_COMMAND_SQL_QUERY = "SELECT * FROM " + DATA_SERVICE_NAME;
  private static final String TEST_COMMAND_SQL_QUERY = CommandExecutor.COMMAND_START + " SELECT * FROM "
    + DATA_SERVICE_NAME;
  private static final int MAX_ROWS = 100;
  private static final IDataServiceClientService.StreamingMode WINDOW_MODE
    = IDataServiceClientService.StreamingMode.ROW_BASED;
  private static final long WINDOW_SIZE = 10;
  private static final long WINDOW_EVERY = 0;
  private static final long WINDOW_MAX_SIZE = 1;

  private CommandQueryService client;

  @Mock DataOutputStream dataOutputStream;
  @Mock OutputStream outputStream;

  @Before
  public void setUp() throws Exception {
    client = new CommandQueryService( context );
  }

  @Test
  public void testExecuteNonCommandQuery() throws Exception {
    Query result = client.prepareQuery( TEST_NON_COMMAND_SQL_QUERY, MAX_ROWS, new HashMap() );
    assertNull( result );
    result = client.prepareQuery( TEST_NON_COMMAND_SQL_QUERY, WINDOW_MODE, WINDOW_SIZE, WINDOW_EVERY, WINDOW_MAX_SIZE,
      new HashMap() );
    assertNull( result );
  }

  @Test
  public void testExecuteCommandQuery() throws Exception {
    Query result = client.prepareQuery( TEST_COMMAND_SQL_QUERY, MAX_ROWS, new HashMap() );
    assertNotNull( result );
    assertEquals( 0, result.getTransList().size() );
    result = client.prepareQuery( TEST_COMMAND_SQL_QUERY, WINDOW_MODE, WINDOW_SIZE, WINDOW_EVERY, WINDOW_MAX_SIZE,
      new HashMap() );
    assertNotNull( result );
    assertEquals( 0, result.getTransList().size() );
  }

  @Test
  public void testAsDataOutputStream() throws IOException {
    assertSame( dataOutputStream, CommandQueryService.asDataOutputStream( dataOutputStream ) );
    DataOutputStream out = CommandQueryService.asDataOutputStream( outputStream );

    out.write( 1 );
    verify( outputStream ).write( 1 );
  }
}
