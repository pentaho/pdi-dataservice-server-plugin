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


package org.pentaho.di.trans.dataservice.optimization.paramgen;

import org.pentaho.metaverse.api.ChangeType;
import org.pentaho.metaverse.api.model.IOperation;
import org.pentaho.metaverse.api.model.Operations;

import java.util.Collections;

import static org.mockito.Mockito.mock;

/**
* @author nhudak
*/
class MockOperations extends Operations {
  MockOperations put( ChangeType type ) {
    return put( type, mock( IOperation.class ) );
  }

  MockOperations put( ChangeType type, IOperation operation ) {
    put( type, Collections.singletonList( operation ) );
    return this;
  }
}
