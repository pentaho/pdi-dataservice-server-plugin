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

package com.pentaho.di.trans.dataservice.optimization.paramgen;

import com.pentaho.di.trans.dataservice.optimization.PushDownType;
import com.pentaho.di.trans.dataservice.optimization.SourceTargetFields;
import org.pentaho.metastore.persist.MetaStoreAttribute;

import java.util.ArrayList;
import java.util.List;

/**
 * @author nhudak
 */
public class ParameterGeneration implements PushDownType {

  public static final String TYPE_NAME = "Parameter Generation";

  // Default to WHERE clause generation
  @MetaStoreAttribute
  private OptimizationForm form = OptimizationForm.WHERE_CLAUSE;

  @MetaStoreAttribute
  private List<SourceTargetFields> fieldMappings = new ArrayList<SourceTargetFields>();

  @MetaStoreAttribute
  private String parameterName;

  public String getParameterName() {
    return parameterName;
  }

  public void setParameterName( String parameterName ) {
    this.parameterName = parameterName;
  }

  @Override public String getTypeName() {
    return TYPE_NAME;
  }

  public OptimizationForm getForm() {
    return form;
  }

  public void setForm( OptimizationForm form ) {
    this.form = form;
  }

  @Override public String getFormName() {
    return form.getFormName();
  }

  public List<SourceTargetFields> getFieldMappings() {
    return fieldMappings;
  }

  public SourceTargetFields createFieldMapping() {
    SourceTargetFields mapping = new SourceTargetFields();
    fieldMappings.add( mapping );
    return mapping;
  }

  public SourceTargetFields removeFieldMapping( SourceTargetFields mapping ) {
    fieldMappings.remove( mapping );
    return mapping;
  }

}
