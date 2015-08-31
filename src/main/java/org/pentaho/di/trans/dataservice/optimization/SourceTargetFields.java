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

package org.pentaho.di.trans.dataservice.optimization;

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.persist.MetaStoreElementType;

@MetaStoreElementType(
  name = "Source Target Fields",
  description = "Defines the mapping of a source to target field and any related metadata.  "
    + "Used for push down optimization."
)
public class SourceTargetFields {

  public static final String SOURCE_FIELD_NAME = "source_field_name";
  public static final String TARGET_FIELD_NAME = "target_field_name";
  public static final Predicate<SourceTargetFields> IS_DEFINED = new Predicate<SourceTargetFields>() {
    @Override public boolean apply( SourceTargetFields input ) {
      return input.isDefined();
    }
  };

  @MetaStoreAttribute( key = SOURCE_FIELD_NAME )
  private String sourceFieldName;

  @MetaStoreAttribute( key = TARGET_FIELD_NAME )
  private String targetFieldName;

  // protected boolean filterAllowed;  // Not used yet.

  public SourceTargetFields() {
  }

  public SourceTargetFields( String source, String target ) {
    setSourceFieldName( source );
    setTargetFieldName( target );
  }

  public String getSourceFieldName() {
    return sourceFieldName;
  }

  public void setSourceFieldName( String sourceFieldName ) {
    this.sourceFieldName = sourceFieldName;
  }

  public String getTargetFieldName() {
    return targetFieldName;
  }

  public void setTargetFieldName( String targetFieldName ) {
    this.targetFieldName = targetFieldName;
  }

  public boolean isDefined() {
    return !Strings.isNullOrEmpty( getSourceFieldName() ) && !Strings.isNullOrEmpty( getTargetFieldName() );
  }

  @Override public String toString() {
    return Objects.toStringHelper( this )
      .add( "sourceFieldName", sourceFieldName )
      .add( "targetFieldName", targetFieldName )
      .toString();
  }
}
