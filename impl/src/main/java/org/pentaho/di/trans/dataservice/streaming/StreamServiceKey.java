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

package org.pentaho.di.trans.dataservice.streaming;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

import java.io.Serializable;
import java.util.Map;

/**
 * Class to represents the Streaming Service key factory.
 */
public class StreamServiceKey implements Serializable {
  private static final long serialVersionUID = 1L;
  /**
   * Required
   */
  private final String dataServiceId;
  /**
   * Required
   */
  private final ImmutableMap<String, String> parameters;

  /**
   * Private Constructor.
   *
   * @param dataServiceId - The data service ID
   * @param parameters - The data service parameters.
   */
  private StreamServiceKey( String dataServiceId, ImmutableMap<String, String> parameters ) {
    this.dataServiceId = dataServiceId;
    this.parameters = parameters;
  }

  /**
   * Key creation factory method.
   *
   * @param dataServiceId - The data service ID
   * @param parameters - The data service parameters.
   * @return
   */
  public static StreamServiceKey create( String dataServiceId, Map<String, String> parameters ) {
    // Copy execution parameters
    ImmutableMap<String, String> params = ImmutableMap.copyOf( parameters );

    return new StreamServiceKey( dataServiceId, params );
  }

  /**
   * Getter for the data service id property.
   *
   * @return - The data service id property.
   */
  public String getDataServiceId() {
    return dataServiceId;
  }

  /**
   * Getter for the parameters property.
   *
   * @return - The parameters property.
   */
  public ImmutableMap<String, String> getParameters() {
    return parameters;
  }

  @Override
  public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    StreamServiceKey cacheKey = (StreamServiceKey) o;
    return Objects.equal( dataServiceId, cacheKey.dataServiceId )
      && Objects.equal( parameters, cacheKey.parameters );
  }

  @Override
  public int hashCode() {
    return Objects.hashCode( dataServiceId, parameters );
  }

  @Override
  public String toString() {
    return Objects.toStringHelper( StreamServiceKey.class )
      .add( "dataServiceId", dataServiceId )
      .add( "parameters", parameters )
      .toString();
  }
}
