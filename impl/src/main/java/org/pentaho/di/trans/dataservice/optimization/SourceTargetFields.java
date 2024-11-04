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


package org.pentaho.di.trans.dataservice.optimization;

import com.google.common.base.MoreObjects;
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
    return MoreObjects.toStringHelper( this )
      .add( "sourceFieldName", sourceFieldName )
      .add( "targetFieldName", targetFieldName )
      .toString();
  }
}
