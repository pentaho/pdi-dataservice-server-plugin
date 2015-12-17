/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.execution;

import com.google.common.collect.ImmutableList;
import org.pentaho.di.core.parameters.NamedParams;
import org.pentaho.di.core.parameters.UnknownParamException;

import java.util.List;
import java.util.Map;

/**
 * @author nhudak
 */
public class CopyParameters implements Runnable {
  private final Map<String, String> parameters;
  private final NamedParams first;
  private final List<NamedParams> rest;

  public CopyParameters( Map<String, String> parameters, NamedParams first, NamedParams... rest ) {
    this.parameters = parameters;
    this.first = first;
    this.rest = ImmutableList.copyOf( rest );
  }

  public void run() {
    for ( Map.Entry<String, String> parameter : parameters.entrySet() ) {
      try {
        first.setParameterValue( parameter.getKey(), parameter.getValue() );
      } catch ( UnknownParamException ignored ) {
      }
    }
    for ( NamedParams namedParams : rest ) {
      namedParams.copyParametersFrom( first );
    }
  }
}
