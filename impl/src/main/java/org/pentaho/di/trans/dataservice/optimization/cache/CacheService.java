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


package org.pentaho.di.trans.dataservice.optimization.cache;

import javax.cache.Cache;

/**
 * Created by bmorrise on 9/15/16.
 */
public interface CacheService {
  Cache getCache( String dataServiceName );
  void destroyCache( String dataServiceName );
}
