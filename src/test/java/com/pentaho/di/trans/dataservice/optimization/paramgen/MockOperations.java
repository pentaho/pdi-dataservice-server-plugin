package com.pentaho.di.trans.dataservice.optimization.paramgen;

import com.pentaho.metaverse.analyzer.kettle.ChangeType;
import com.pentaho.metaverse.api.model.IOperation;
import com.pentaho.metaverse.api.model.Operations;

import java.util.Collections;

import static org.mockito.Mockito.mock;

/**
* @author nhudak
*/
class MockOperations extends Operations {
  MockOperations put( ChangeType type ) {
    return put( type, mock( IOperation.class ) );
  }

  MockOperations put( ChangeType type, IOperation operation ){
    put( type, Collections.singletonList( operation ) );
    return this;
  }
}
