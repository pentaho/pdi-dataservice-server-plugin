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
import org.pentaho.di.trans.dataservice.optimization.OptimizationImpactInfo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
  private final Map<String, String> parameters;

  private final List<String> optimizations;

  /**
   * Private Constructor.
   *
   * @param dataServiceId - The data service ID
   * @param parameters - The data service parameters.
   */
  private StreamServiceKey( String dataServiceId, Map<String, String> parameters, List<String> optimizations ) {
    this.dataServiceId = dataServiceId;
    this.parameters = parameters;
    this.optimizations = optimizations;
  }

  /**
   * Key creation factory method.
   *
   * @param dataServiceId - The data service ID.
   * @param parameters - The data service parameters.
   * @param optimizationList - The data service optimizations.
   * @return - The key for the given arguments.
   */
  public static StreamServiceKey create( String dataServiceId, Map<String, String> parameters, List<OptimizationImpactInfo> optimizationList ) {
    // Copy execution parameters (to be really unmodifiable, the below must be done - the unmodifiable references
    // the argument list internally, allowing it to be changed outside and hence having it's contents also modified)
    Map<String, String> params = Collections.unmodifiableMap( new HashMap<>( parameters ) );

    List<String> optimizations = new ArrayList<>();
    if ( optimizationList != null ) {
      for ( OptimizationImpactInfo optimizationImpact : optimizationList ) {
        if ( optimizationImpact.getQueryAfterOptimization() != null && !optimizationImpact.getQueryAfterOptimization().isEmpty() ) {
          optimizations.add( optimizationImpact.getQueryAfterOptimization() );
        }
      }
    }
    return new StreamServiceKey( dataServiceId, params, optimizations );
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
  public Map<String, String> getParameters() {
    return parameters;
  }

  public List<String> getOptimizations() {
    return optimizations;
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
      && Objects.equal( parameters, cacheKey.parameters )
      && Objects.equal( optimizations, cacheKey.optimizations );
  }

  @Override
  public int hashCode() {
    return Objects.hashCode( dataServiceId, parameters, optimizations );
  }

  @Override
  public String toString() {
    return Objects.toStringHelper( StreamServiceKey.class )
      .add( "dataServiceId", dataServiceId )
      .add( "parameters", parameters )
      .add( "optimizations", optimizations )
      .toString();
  }
}
