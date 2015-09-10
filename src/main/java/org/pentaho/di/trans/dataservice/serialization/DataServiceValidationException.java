package org.pentaho.di.trans.dataservice.serialization;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.dataservice.DataServiceMeta;

/**
 * @author nhudak
 */
public class DataServiceValidationException extends KettleException {
  protected final DataServiceMeta dataServiceMeta;

  public DataServiceValidationException( DataServiceMeta dataServiceMeta, String message ) {
    super( message );
    this.dataServiceMeta = dataServiceMeta;
  }

  public DataServiceMeta getDataServiceMeta() {
    return dataServiceMeta;
  }
}
