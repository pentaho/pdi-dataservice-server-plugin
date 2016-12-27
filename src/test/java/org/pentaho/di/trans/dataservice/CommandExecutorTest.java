/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.internal.verification.VerificationModeFactory;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Created by bmorrise on 12/10/15.
 */
@RunWith( MockitoJUnitRunner.class ) public class CommandExecutorTest {

  private static final String EXECUTOR_ID = "12345";
  private static final String
      STOP_COMMAND =
      CommandExecutor.buildCommand( CommandExecutor.StopCommand.STOP, EXECUTOR_ID );
  private static final String
      ERRORS_COMMAND =
      CommandExecutor.buildCommand( CommandExecutor.ErrorsCommand.ERRORS, EXECUTOR_ID );

  @Mock DataServiceContext context;

  @Mock DataServiceExecutor executor;

  @Mock DataServiceMeta dataServiceMeta;

  @Before public void setUp() {
    when( context.getExecutor( EXECUTOR_ID ) ).thenReturn( executor );
    when( executor.getService() ).thenReturn( dataServiceMeta );
  }

  @Test public void testBuildCommand() {
    assertThat( CommandExecutor.buildCommand( "test", "12345", "6789" ), equalTo( "[ test 12345 6789 ]" ) );
  }

  @Test public void testExecuteStop() throws Exception {
    CommandExecutor commandExecutor = new CommandExecutor.Builder( STOP_COMMAND, context ).build();
    when( dataServiceMeta.isUserDefined() ).thenReturn( true );

    DataInputStream dataInputStream = executeCommand( commandExecutor );

    verify( executor ).stop();

    assertThat( dataInputStream.readUTF(), equalTo( "true" ) );
  }

  @Test public void testExecuteStopOnTransient() throws Exception {
    CommandExecutor commandExecutor = new CommandExecutor.Builder( STOP_COMMAND, context ).build();
    when( dataServiceMeta.isUserDefined() ).thenReturn( false );

    DataInputStream dataInputStream = executeCommand( commandExecutor );

    verify( executor, never() ).stop();

    assertThat( dataInputStream.readUTF(), equalTo( "false" ) );
  }

  @Test public void testExecuteErrors() throws Exception {
    when( executor.hasErrors() ).thenReturn( true );
    CommandExecutor commandExecutor = new CommandExecutor.Builder( ERRORS_COMMAND, context ).build();

    DataInputStream dataInputStream = executeCommand( commandExecutor );

    verify( executor ).hasErrors();

    assertThat( dataInputStream.readUTF(), equalTo( "true" ) );
  }

  private static DataInputStream executeCommand( CommandExecutor commandExecutor ) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream( baos );

    commandExecutor.execute( dos );

    ByteArrayInputStream bais = new ByteArrayInputStream( baos.toByteArray() );
    return new DataInputStream( bais );
  }


}
