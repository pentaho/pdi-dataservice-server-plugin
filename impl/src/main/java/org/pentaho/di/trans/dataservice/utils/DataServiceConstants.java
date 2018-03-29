/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.utils;

import java.util.concurrent.TimeUnit;

/**
 * General constants.
 */
public final class DataServiceConstants {
  @Deprecated
  public static final String LEGACY_LIMIT_PROPERTY = "det.dataservice.dynamic.limit";
  public static final String ROW_LIMIT_PROPERTY = "dataservice.dynamic.limit";
  public static final String TIME_LIMIT_PROPERTY = "dataservice.dynamic.timelimitmilli";
  public static final int ROW_LIMIT_DEFAULT = 50000;
  public static final long TIME_LIMIT_DEFAULT = 100000;

  // Logging constants
  public static final String PASSING_ALONG_ROW = "Passing along row: ";
  public static final String ROW_BUFFER_IS_FULL_TRYING_AGAIN = "Row buffer is full, trying again";
  public static final String STREAMING_TRANSFORMATION_STOPPED = "Streaming transformation stopped";
  public static final String STREAMING_GENERATED_TRANSFORMATION_STOPPED = "Generated transformation stopped";
  public static final String STREAMING_CACHE_REMOVED = "Streaming cache listener removed: ";

  // Streaming cache management
  public static final long STREAMING_CACHE_DURATION = 120;
  public static final TimeUnit STREAMING_CACHE_TIME_UNIT = TimeUnit.SECONDS;
  public static final long STREAMING_CACHE_CLEANUP_INTERVAL_SECONDS = 130;
  public static final int KETTLE_STREAMING_ROW_LIMIT = 5000;
  public static final int KETTLE_STREAMING_TIME_LIMIT = 10000;

  private DataServiceConstants() {
    throw new AssertionError();
  }
}
