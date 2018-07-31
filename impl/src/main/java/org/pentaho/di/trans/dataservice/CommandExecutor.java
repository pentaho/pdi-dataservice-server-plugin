/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by bmorrise on 12/9/15.
 */
public class CommandExecutor {

  public static final String COMMAND_START = "[";
  public static final String COMMAND_END = "]";

  private Map<String, Command> commands = new HashMap<>();
  private String func;
  private String[] args;

  private CommandExecutor( Builder builder ) {
    func = builder.func;
    args = builder.args;

    commands.put( StopCommand.STOP, new StopCommand( builder.context ) );
    commands.put( ErrorsCommand.ERRORS, new ErrorsCommand( builder.context ) );
  }

  public static class Builder {

    private String command;
    private DataServiceContext context;
    private String func;
    private String[] args;

    public Builder( String command, DataServiceContext context ) {
      this.command = command;
      this.context = context;
    }

    public CommandExecutor build() {
      String[]
          parts =
          command.substring( COMMAND_START.length(), command.length() - COMMAND_END.length() ).trim().split( " " );
      func = parts[0];
      args = Arrays.copyOfRange( parts, 1, parts.length );

      return new CommandExecutor( this );
    }
  }

  public static String buildCommand( String command, String... args ) {
    StringBuilder builder = new StringBuilder();
    builder.append( COMMAND_START );
    builder.append( " " );
    builder.append( command );
    builder.append( " " );
    for ( String arg : args ) {
      builder.append( arg );
      builder.append( " " );
    }
    builder.append( COMMAND_END );

    return builder.toString();
  }

  public void execute( DataOutputStream dataOutputStream ) throws IOException {
    Command command = commands.get( func );
    if ( command != null ) {
      String result = command.execute( args );
      dataOutputStream.writeUTF( result );
    }
  }

  private interface Command {
    String execute( String[] args );
  }

  private abstract class ExecutorCommand implements Command {
    protected final DataServiceContext context;

    private ExecutorCommand( DataServiceContext context ) {
      this.context = context;
    }
  }


  public class StopCommand extends ExecutorCommand {
    public static final String STOP = "stop";

    public StopCommand( DataServiceContext context ) {
      super( context );
    }

    @Override public String execute( String[] args ) {
      DataServiceExecutor executor = context.getExecutor( args[0] );
      if ( executor != null ) {
        executor.stop( false );
        context.removeExecutor( args[0] );
        return "true";
      }
      return "false";
    }
  }

  public class ErrorsCommand extends ExecutorCommand {
    public static final String ERRORS = "errors";

    public ErrorsCommand( DataServiceContext context ) {
      super( context );
    }

    @Override public String execute( String[] args ) {
      DataServiceExecutor executor = context.getExecutor( args[0] );
      if ( executor != null && executor.hasErrors() ) {
        return "true";
      }
      return "false";
    }
  }

}
