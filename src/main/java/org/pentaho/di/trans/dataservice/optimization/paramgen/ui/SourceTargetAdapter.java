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
