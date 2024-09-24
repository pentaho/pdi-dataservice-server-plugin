/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.optimization.paramgen.ui;

import org.pentaho.di.trans.dataservice.optimization.SourceTargetFields;
import org.pentaho.ui.xul.XulEventSourceAdapter;

/**
 * @author nhudak
 */
public class SourceTargetAdapter extends XulEventSourceAdapter {

  private final SourceTargetFields sourceTargetFields;

  public SourceTargetAdapter( SourceTargetFields sourceTargetFields ) {
    this.sourceTargetFields = sourceTargetFields;
  }

  public boolean isDefined() {
    return sourceTargetFields.isDefined();
  }

  public String getSourceFieldName() {
    return sourceTargetFields.getSourceFieldName();
  }

  public void setSourceFieldName( String sourceFieldName ) {
    sourceTargetFields.setSourceFieldName( sourceFieldName );
    propertyChanged();
  }

  public String getTargetFieldName() {
    return sourceTargetFields.getTargetFieldName();
  }

  public void setTargetFieldName( String targetFieldName ) {
    sourceTargetFields.setTargetFieldName( targetFieldName );
    propertyChanged();
  }

  protected void propertyChanged() {
    // Do nothing by default
  }
}
