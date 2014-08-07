package com.pentaho.di.purge;

import org.apache.log4j.spi.LoggingEvent;

/**
 * Provides a common interface for implementation of the Purge Utility Log File.  The implementation
 * should extend <code>org.apache.log4j.Layout</code>
 * @author tkafalas
 *
 */
public interface IPurgeUtilityLayout {
  /**
   * Set the title of the log file
   */
  void setTitle( String title );

  /**
   * Returns the current value of the <b>Title</b> option.
   */
  String getTitle();

  /**
   * Returns the content type output by this layout, i.e "text/html".
   */
  String getContentType();

  /**
   * Format the log line for output
   */
  String format( LoggingEvent event );

  /**
   * Returns appropriate header text for the log.
   */
  String getHeader();

  /**
   * Returns the appropriate footer text for the log.
   */
  String getFooter();

  /**
   * return <code>false</code> if the layout processes a throwable in the logging event, returns <code>true</code>
   * otherwise.
   */
  boolean ignoresThrowable();
}
