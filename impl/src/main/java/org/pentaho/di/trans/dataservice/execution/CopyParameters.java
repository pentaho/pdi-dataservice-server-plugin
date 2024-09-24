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
        // Ignore unknown parameters
      }
    }
    for ( NamedParams namedParams : rest ) {
      namedParams.copyParametersFrom( first );
    }
  }
}
