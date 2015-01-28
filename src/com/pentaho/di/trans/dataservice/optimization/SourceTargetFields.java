/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2014 Pentaho Corporation (Pentaho). All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Pentaho and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Pentaho and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Pentaho is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Pentaho,
 * explicitly covering such access.
 */

package com.pentaho.di.trans.dataservice.optimization;

import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.persist.MetaStoreElementType;


@MetaStoreElementType(
  name = "Source Target Fields",
  description = "Defines the mapping of a source to target field and any related metadata.  "
    +  "Used for push down optimization."
)
public class SourceTargetFields {

  public static final String SOURCE_FIELD_NAME = "source_field_name";
  public static final String TARGET_FIELD_NAME = "target_field_name";

  @MetaStoreAttribute( key = SOURCE_FIELD_NAME )
  private String sourceFieldName;

  @MetaStoreAttribute( key = TARGET_FIELD_NAME )
  private String targetFieldName;

  // protected boolean filterAllowed;  // Not used yet.

  public SourceTargetFields() { }

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




}
