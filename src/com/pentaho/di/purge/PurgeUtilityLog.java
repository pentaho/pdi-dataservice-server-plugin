/*
 * This program is free software; you can redistribute it and/or modify it under the
 * terms of the GNU General Public License, version 2 as published by the Free Software
 * Foundation.
 *
 * You should have received a copy of the GNU General Public License along with this
 * program; if not, you can obtain a copy at http://www.gnu.org/licenses/gpl-2.0.html
 * or from the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 *
 *
 * Copyright 2006 - 2013 Pentaho Corporation.  All rights reserved.
 */

package com.pentaho.di.purge;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.apache.log4j.WriterAppender;

public class PurgeUtilityLog {

  private Logger logger;
  static final String FILE_KEY = "currentFile"; // Intentionally scoped as default
  private OutputStream outputStream;
  private String currentFilePath;
  private String logName;
  private String purgePath;
  private Level logLevel;
  private WriterAppender writeAppender;
  
  /**
   * Constructs this object when when no some method that uses the logger is executed but there is no formal log.
   * When this occurs the log returned is just a simple Log4j logger.
   */
  PurgeUtilityLog( ) {

  }

  /**
   * Constructs an object that keeps track of additional fields for Log4j logging and writes/formats an html file to the
   * output stream provided.
   * 
   * @param outputStream
   */
  PurgeUtilityLog( OutputStream outputStream, String purgePath, Level logLevel ) {
    this.outputStream = outputStream;
    this.purgePath = purgePath;
    this.logLevel = logLevel;
    init();
  }

  private void init() {
    logName = "PurgeUtilityLog." + getThreadName();
    logger = Logger.getLogger( logName );
    logger.setLevel( logLevel );
    PurgeUtilityHTMLLayout htmlLayout = new PurgeUtilityHTMLLayout( logLevel );
    htmlLayout.setTitle( "Purge Utility Log" );
    writeAppender = new WriterAppender( htmlLayout, new OutputStreamWriter( outputStream, Charset.forName( "utf-8" ) ) );
    logger.addAppender( writeAppender );
  }

  public Logger getLogger() {
    if ( logger == null )
      return Logger.getLogger( Thread.currentThread().getStackTrace()[4].getClassName() );
    else {
      return logger;
    }
  }

  /**
   * @return the currentFilePath
   */
  public String getCurrentFilePath() {
    return currentFilePath;
  }

  /**
   * @param currentFilePath
   *          the currentFilePath to set
   */
  public void setCurrentFilePath( String currentFilePath ) {
    this.currentFilePath = currentFilePath;
    MDC.put( FILE_KEY, currentFilePath );
  }
  
  /**
   * @return the purgePath
   */
  public String getPurgePath() {
    return purgePath;
  }

  protected void endJob() {
    try {
      outputStream.write( writeAppender.getLayout().getFooter().getBytes() );
    } catch ( Exception e ) {
      System.out.println( e );
      // Don't try logging a log error.
    }
    logger.removeAppender( logName );
  }

  private String getThreadName() {
    return Thread.currentThread().getName();
  }

}
