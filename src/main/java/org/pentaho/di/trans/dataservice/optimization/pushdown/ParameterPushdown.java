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

package org.pentaho.di.trans.dataservice.optimization.pushdown;

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.pentaho.di.core.parameters.DuplicateParamException;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.optimization.OptimizationImpactInfo;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownType;
import org.pentaho.metastore.persist.MetaStoreAttribute;

import java.util.Iterator;
import java.util.List;

/**
 * @author nhudak
 */
public class ParameterPushdown implements PushDownType {
  static final String NAME = "Parameter Capture";

  public static final String DEFINITIONS_LIST_ATTRIBUTE = "definitions_list";
  public static final String PARAMETER_ATTRIBUTE = "parameter_name";
  public static final String FIELD_NAME_ATTRIBUTE = "column_name";
  public static final String FORMAT_ATTRIBUTE = "parameter_format";
  public static final String DEFAULT_FORMAT = "%s";

  @MetaStoreAttribute( key = DEFINITIONS_LIST_ATTRIBUTE )
  private final List<Definition> definitions = Lists.newArrayList();

  public List<Definition> getDefinitions() {
    return definitions;
  }

  public Definition createDefinition() {
    Definition definition = new Definition();
    definitions.add( definition );
    return definition;
  }

  @Override public void init( TransMeta transMeta, DataServiceMeta dataService, PushDownOptimizationMeta optMeta ) {
    optMeta.setStepName( dataService.getStepname() );

    for ( Iterator<Definition> iterator = definitions.iterator(); iterator.hasNext(); ) {
      Definition definition = iterator.next();
      if ( definition.isValid() ) {
        try {
          transMeta.addParameterDefinition( definition.getParameter(), "", "Data Service Field Parameter" );
        } catch ( DuplicateParamException e ) {
          // Ignore duplicates
        }
      } else {
        iterator.remove();
      }
    }
    transMeta.activateParameters();
  }

  @Override
  public ListenableFuture<Boolean> activate( DataServiceExecutor executor, PushDownOptimizationMeta meta ) {
    return Futures.immediateFuture( false );
  }

  @Override public OptimizationImpactInfo preview( DataServiceExecutor executor, PushDownOptimizationMeta meta ) {
    return new OptimizationImpactInfo( meta.getStepName() );
  }

  public static class Definition {

    @MetaStoreAttribute( key = PARAMETER_ATTRIBUTE )
    private String parameter;

    @MetaStoreAttribute( key = FORMAT_ATTRIBUTE )
    private String format = DEFAULT_FORMAT;

    @MetaStoreAttribute( key = FIELD_NAME_ATTRIBUTE )
    private String fieldName;

    public String getFieldName() {
      return fieldName;
    }

    public void setFieldName( String column ) {
      this.fieldName = column;
    }

    public String getParameter() {
      return parameter;
    }

    public void setParameter( String parameter ) {
      this.parameter = parameter;
    }

    public String getFormat() {
      if ( Strings.isNullOrEmpty( format ) ) {
        format = DEFAULT_FORMAT;
      }
      return format;
    }

    public void setFormat( String format ) {
      this.format = format;
    }

    @Override public String toString() {
      return Objects.toStringHelper( this )
        .add( "fieldName", getFieldName() )
        .add( "parameter", getParameter() )
        .add( "format", getFormat() )
        .toString();
    }

    public boolean isValid() {
      return !Strings.isNullOrEmpty( getParameter() ) && !Strings.isNullOrEmpty( getFieldName() );
    }
  }
}
