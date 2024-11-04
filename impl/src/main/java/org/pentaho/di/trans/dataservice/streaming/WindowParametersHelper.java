/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package org.pentaho.di.trans.dataservice.streaming;

import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;

/**
 * This class contains static methods that help on dealing with the window parameters.
 * The windowMode is the query window mode.
 * The windowSize is the query window size.
 * The windowEvery is the query window rate.
 * The windowMaxRowLimit is the query window max rows.
 * The windowMaxTimeLimit is the query window max time.
 * The windowLimit is the query window limit.
 */
public abstract class WindowParametersHelper {

  /**
   * Generates the cache key for a given query with a specific size and rate.
   *
   * @param query The query.
   * @param windowMode The query window mode.
   * @param windowSize The query window size.
   * @param windowEvery The query window rate.
   * @param windowMaxRowLimit The query window max rows.
   * @param windowMaxTimeLimit The query window max time.
   * @param windowLimit The query window limit.
   * @param serviceExecutorCacheKeyHash The service transformation service executor key cache hash.
   * @return The cache key for the query.
   */
  public static String getCacheKey( String query, IDataServiceClientService.StreamingMode windowMode, long windowSize, long windowEvery,
                                    int windowMaxRowLimit, long windowMaxTimeLimit, long windowLimit, int serviceExecutorCacheKeyHash ) {

    boolean timeBased = IDataServiceClientService.StreamingMode.TIME_BASED.equals( windowMode );
    boolean rowBased = IDataServiceClientService.StreamingMode.ROW_BASED.equals( windowMode );

    int maxRows = getMaxRows( windowMaxRowLimit, windowLimit, timeBased );
    long maxTime = getMaxTime( windowMaxTimeLimit, windowLimit, rowBased );

    windowSize = getWindowSize( windowSize, timeBased, maxRows, maxTime );
    if ( windowSize == 0 ) {
      return null;
    }

    windowEvery = getWindowEvery( windowEvery, timeBased, maxRows, maxTime );

    return String.valueOf( serviceExecutorCacheKeyHash )
      .concat( "-" ).concat( query.concat( windowMode.toString() ) )
      .concat( "-" ).concat( String.valueOf( windowSize ) )
      .concat( "-" ).concat( String.valueOf( windowEvery ) )
      .concat( "-" ).concat( String.valueOf( maxRows ) )
      .concat( "-" ).concat( String.valueOf( maxTime ) );
  }

  /**
   * Applies a majority logic to the window every value, based on the max rows/time.
   * @param windowEvery The window every value. It's value is internally set to 0, if a negative value is used.
   * @param timeBased Indicates if the window is time based.
   * @param maxRows The maximum rows limit (used when the timeBased is false).
   * @param maxTime The maximum time limit (used when the timeBased is true).
   * @return The minimum between windowEvery and maxTime, when timBased is true. The minimum between windowEvery and maxRows, otherwise.
   */
  public static long getWindowEvery( long windowEvery, boolean timeBased, int maxRows, long maxTime ) {
    windowEvery = windowEvery <= 0 ? 0
      : ( timeBased ? Math.min( windowEvery, maxTime ) : Math.min( windowEvery, maxRows ) );
    return windowEvery;
  }

  /**
   * Applies a majority logic to the window size value, based on the max rows/time.
   * @param windowSize The window size value. It's value is internally set to 0, if a negative value is used.
   * @param timeBased Indicates if the window is time based.
   * @param maxRows The maximum rows limit (used when the timeBased is false).
   * @param maxTime The maximum time limit (used when the timeBased is true).
   * @return The minimum between windowSize and maxTime, when timBased is true. The minimum between windowSize and maxRows, otherwise.
   */
  public static long getWindowSize( long windowSize, boolean timeBased, int maxRows, long maxTime ) {
    windowSize = windowSize <= 0 ? 0
      : ( timeBased ? Math.min( windowSize, maxTime ) : Math.min( windowSize, maxRows ) );
    return windowSize;
  }

  /**
   * Get the max time allowed for the generation of a window
   * @param windowMaxTimeLimit The query window max time.
   * @param windowLimit The query window limit.
   * @param rowBased Indicates if the query is row based.
   * @return the maximum time allowed.
   */
  public static long getMaxTime( long windowMaxTimeLimit, long windowLimit, boolean rowBased ) {
    return windowLimit > 0 && rowBased ? Math.min( windowLimit, windowMaxTimeLimit )
        : windowMaxTimeLimit;
  }

  /**
   * Get the max rows allowed for the generation of a window
   * @param windowMaxRowLimit The query window max rows.
   * @param windowLimit The query window limit.
   * @param timeBased Indicates if the query is time based.
   * @return the maximum rows allowed.
   */
  public static int getMaxRows( int windowMaxRowLimit, long windowLimit, boolean timeBased ) {
    return windowLimit > 0 && timeBased ? (int) Math.min( windowLimit, windowMaxRowLimit )
        : windowMaxRowLimit;
  }
}
