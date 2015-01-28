package com.pentaho.di.trans.dataservice.optimization;

public class OptimizationImpactInfo {
  private String queryBeforeOptimization = "";
  private String queryAfterOptimization = "";
  private String stepName = "";
  private boolean modified;
  private String errorMsg = "";


  public String getQueryBeforeOptimization() {
    return queryBeforeOptimization == null || queryBeforeOptimization.trim().length() == 0
      ? "<no query>" : queryBeforeOptimization;
  }

  public void setQueryBeforeOptimization( String queryBeforeOptimization ) {
    this.queryBeforeOptimization = queryBeforeOptimization;
  }

  public String getQueryAfterOptimization() {
    return queryAfterOptimization;
  }

  public void setQueryAfterOptimization( String queryAfterOptimization ) {
    this.queryAfterOptimization = queryAfterOptimization;
  }

  public String getStepName() {
    return stepName;
  }

  public void setStepName( String stepName ) {
    this.stepName = stepName;
  }

  public boolean isModified() {
    return modified;
  }

  public void setModified( boolean modified ) {
    this.modified = modified;
  }

  public String getErrorMsg() {
    return errorMsg;
  }

  public void setErrorMsg( String errorMsg ) {
    this.errorMsg = errorMsg;
  }
}
