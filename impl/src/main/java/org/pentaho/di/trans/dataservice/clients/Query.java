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

package org.pentaho.di.trans.dataservice.clients;

import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;

import io.reactivex.Observer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

/**
 * @author nhudak
 */
public interface Query {

  void writeTo( OutputStream outputStream ) throws IOException;

  List<Trans> getTransList();

  default void pushTo( Observer<List<RowMetaAndData>> streamingWindowObserver ) throws Exception {
    throw new UnsupportedOperationException();
  }

  interface Service {
    Query prepareQuery( String sql, int maxRows, Map<String, String> parameters ) throws KettleException;
    Query prepareQuery( String sql, IDataServiceClientService.StreamingMode windowMode,
                        long windowSize, long windowEvery, long windowLimit,
                        Map<String, String> parameters ) throws KettleException;
  }
}
