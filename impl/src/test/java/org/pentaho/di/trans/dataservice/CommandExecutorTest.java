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


package org.pentaho.di.trans.dataservice;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bmorrise on 12/10/15.
 */
@RunWith( MockitoJUnitRunner.StrictStubs.class)
public class CommandExecutorTest {

  private static final String EXECUTOR_ID = "12345";
  private static final String
      STOP_COMMAND =
      CommandExecutor.buildCommand( CommandExecutor.StopCommand.STOP, EXECUTOR_ID );
  private static final String
      ERRORS_COMMAND =
      CommandExecutor.buildCommand( CommandExecutor.ErrorsCommand.ERRORS, EXECUTOR_ID );

  @Mock DataServiceContext context;

  @Mock DataServiceExecutor executor;

  @Before public void setUp() {
    when( context.getExecutor( EXECUTOR_ID ) ).thenReturn( executor );
  }

  @Test public void testBuildCommand() {
    assertThat( CommandExecutor.buildCommand( "test", "12345", "6789" ), equalTo( "[ test 12345 6789 ]" ) );
  }

  @Test public void testExecuteStop() throws Exception {
    CommandExecutor commandExecutor = new CommandExecutor.Builder( STOP_COMMAND, context ).build();
    DataInputStream dataInputStream = executeCommand( commandExecutor );
    verify( context ).removeExecutor( EXECUTOR_ID );
    assertThat( dataInputStream.readUTF(), equalTo( "true" ) );
  }

  @Test public void testExecuteStopFalse() throws Exception {
    doReturn( null ).when( context ).getExecutor( anyString() );
    CommandExecutor commandExecutor = new CommandExecutor.Builder( STOP_COMMAND, context ).build();
    DataInputStream dataInputStream = executeCommand( commandExecutor );
    verify( context, times( 0 ) ).removeExecutor( EXECUTOR_ID );
    assertThat( dataInputStream.readUTF(), equalTo( "false" ) );
  }

  @Test public void testExecuteErrors() throws Exception {
    when( executor.hasErrors() ).thenReturn( true );
    CommandExecutor commandExecutor = new CommandExecutor.Builder( ERRORS_COMMAND, context ).build();
    DataInputStream dataInputStream = executeCommand( commandExecutor );
    verify( executor ).hasErrors();
    assertThat( dataInputStream.readUTF(), equalTo( "true" ) );
  }

  @Test public void testExecuteErrorsFalse() throws Exception {
    doReturn( null ).when( context ).getExecutor( anyString() );
    when( executor.hasErrors() ).thenReturn( true );
    CommandExecutor commandExecutor = new CommandExecutor.Builder( ERRORS_COMMAND, context ).build();
    DataInputStream dataInputStream = executeCommand( commandExecutor );
    verify( executor, times( 0 ) ).hasErrors();
    assertTrue( executor.hasErrors() );
    assertThat( dataInputStream.readUTF(), equalTo( "false" ) );
  }

  @Test public void testExecuteErrorsFalseNoErrors() throws Exception {
    when( executor.hasErrors() ).thenReturn( false );
    CommandExecutor commandExecutor = new CommandExecutor.Builder( ERRORS_COMMAND, context ).build();
    DataInputStream dataInputStream = executeCommand( commandExecutor );
    verify( executor, times( 1 ) ).hasErrors();
    assertThat( dataInputStream.readUTF(), equalTo( "false" ) );
  }

  private static DataInputStream executeCommand( CommandExecutor commandExecutor ) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream( baos );

    commandExecutor.execute( dos );

    ByteArrayInputStream bais = new ByteArrayInputStream( baos.toByteArray() );
    return new DataInputStream( bais );
  }


}
