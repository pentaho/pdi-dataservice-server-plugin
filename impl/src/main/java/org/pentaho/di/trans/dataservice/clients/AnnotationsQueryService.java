/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.di.trans.dataservice.clients;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.pentaho.agilebi.modeler.models.annotations.ModelAnnotationGroup;
import org.pentaho.agilebi.modeler.models.annotations.ModelAnnotationGroupXmlWriter;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;
import org.pentaho.di.trans.dataservice.resolvers.DataServiceResolver;
import org.pentaho.di.trans.step.StepMetaDataCombi;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.osgi.metastore.locator.api.MetastoreLocator;

import static org.pentaho.di.trans.dataservice.clients.TransMutators.disableAllUnrelatedHops;


public class AnnotationsQueryService implements Query.Service {
  private MetastoreLocator metastoreLocator;
  private DataServiceResolver resolver;

  public AnnotationsQueryService( final MetastoreLocator metastoreLocator, final DataServiceResolver resolver ) {
    this.metastoreLocator = metastoreLocator;
    this.resolver = resolver;
  }

  @Override public Query prepareQuery( final String sql, final int maxRows, final Map<String, String> parameters )
    throws KettleException {
    return prepareQuery( sql, null, 0, 0, 0, parameters );
  }

  @Override public Query prepareQuery( final String sql, IDataServiceClientService.StreamingMode windowMode,
                                       long windowSize, long windowEvery, long windowLimit,
                                       final Map<String, String> parameters )
    throws KettleException {
    String prefix = "show annotations from ";
    if ( sql.startsWith( prefix.toLowerCase() ) ) {
      return new AnnotationsQuery( sql.substring( prefix.length() ) );
    }
    return null;
  }

  class AnnotationsQuery implements Query {
    private String serviceName;

    public AnnotationsQuery( final String serviceName ) {
      this.serviceName = serviceName;
    }

    @Override public void writeTo( final OutputStream outputStream ) throws IOException {
      Trans trans = null;
      try {
        trans = prepareExecution();
        ModelAnnotationGroup modelAnnotations =
          (ModelAnnotationGroup) trans.getExtensionDataMap().get( "KEY_MODEL_ANNOTATIONS" );
        writeAnnotations( outputStream, modelAnnotations );
      } catch ( MetaStoreException | KettleException e ) {
        String msg = "Error while executing 'show annotations from " + serviceName + "'";

        //not including original exception here because we don't want that info going back to the jdbc client
        throw new IOException( msg );
      } finally {
        if ( null != trans ) {
          // Dispose resources
          trans.cleanup();
        }
      }
    }

    private void writeAnnotations( final OutputStream outputStream, final ModelAnnotationGroup modelAnnotations )
      throws IOException, KettleFileException {
      ModelAnnotationGroupXmlWriter writer =
        new ModelAnnotationGroupXmlWriter( modelAnnotations );
      DataOutputStream dos = new DataOutputStream( outputStream );

      DataServiceExecutor.writeMetadata( dos, serviceName, "", "", "", "" );

      RowMeta rowMeta = new RowMeta();
      rowMeta.addValueMeta( new ValueMetaString( "annotations" ) );
      rowMeta.writeMeta( dos );

      Object[] row = new Object[] { writer.getXML().trim() };
      rowMeta.writeData( dos, row );
    }

    private Trans prepareExecution() throws MetaStoreException, KettleException {
      DataServiceMeta dataService = resolver.getDataService( serviceName );
      if ( dataService == null ) {
        throw new MetaStoreException( "Unable to load dataservice " + serviceName );
      }
      TransMeta serviceTrans = dataService.getServiceTrans();

      disableAllUnrelatedHops( dataService.getStepname(), serviceTrans, false );
      final Trans trans = getTrans( serviceTrans );
      trans.setMetaStore( metastoreLocator.getMetastore() );
      if ( !serviceTrans.getTransHopSteps( false ).isEmpty() ) {
        trans.prepareExecution( new String[] {} );
      }
      return trans;
    }

    Trans getTrans( final TransMeta serviceTrans ) {
      return new Trans( serviceTrans );
    }

    @Override public List<Trans> getTransList() {
      return Collections.emptyList();
    }
  }
}
