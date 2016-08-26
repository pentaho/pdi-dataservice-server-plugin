package org.pentaho.di.trans.dataservice.clients;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.pentaho.agilebi.modeler.models.annotations.ModelAnnotationGroup;
import org.pentaho.agilebi.modeler.models.annotations.ModelAnnotationGroupXmlWriter;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.serialization.DataServiceFactory;
import org.pentaho.di.trans.step.StepMeta;
/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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
import org.pentaho.metastore.api.exceptions.MetaStoreException;


public class AnnotationsQueryService implements Query.Service {
  private DataServiceFactory factory;

  public AnnotationsQueryService( final DataServiceFactory factory ) {
    this.factory = factory;
  }

  @Override public Query prepareQuery( final String sql, final int maxRows, final Map<String, String> parameters )
    throws KettleException {
    String prefix = "show annotations from ";
    if ( sql.startsWith( prefix ) ) {
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
      try {
        ModelAnnotationGroup modelAnnotations =
          (ModelAnnotationGroup) prepareExecution().getExtensionDataMap().get( "KEY_MODEL_ANNOTATIONS" );
        writeAnnotations( outputStream, modelAnnotations );
      } catch ( MetaStoreException | KettleException e ) {
        String msg = "Error while executing 'show annotations from " + serviceName + "'";
        factory.getLogChannel().logError( msg, e );

        //not including original execption here because we don't want that info going back to the jdbc client
        throw new IOException( msg );
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

      Object[] row = new Object[] { writer.getXML() };
      rowMeta.writeData( dos, row );
    }

    private Trans prepareExecution() throws MetaStoreException, KettleException {
      DataServiceMeta dataService = factory.getDataService( serviceName );
      TransMeta serviceTrans = dataService.getServiceTrans();
      disableAllUnrelatedHops( dataService, serviceTrans );
      final Trans trans = getTrans( serviceTrans );
      trans.setMetaStore( factory.getMetaStore() );
      if ( serviceTrans.getTransHopSteps( false ).size() > 0 ) {
        trans.prepareExecution( new String[]{} );
      }
      return trans;
    }

    private void disableAllUnrelatedHops( final DataServiceMeta dataService, final TransMeta serviceTrans ) {
      String stepname = dataService.getStepname();
      StepMeta step = serviceTrans.findStep( stepname );
      List<TransHopMeta> transHops = new ArrayList<TransHopMeta>( serviceTrans.nrTransHops() );

      for ( int i = 0; i < serviceTrans.nrTransHops(); i++ ) {
        transHops.add( serviceTrans.getTransHop( i ) );
      }
      HashSet<TransHopMeta> hops = new HashSet<>();
      findUpstreamHops( hops, transHops, serviceTrans, step );

      for ( TransHopMeta transHopMeta : transHops ) {
        if ( !hops.contains( transHopMeta ) ) {
          transHopMeta.setEnabled( false );
        }
      }
    }

    private void findUpstreamHops( Set<TransHopMeta> hops, List<TransHopMeta> all, TransMeta trans, StepMeta step ) {
      for ( TransHopMeta hop : all ) {
        if ( hop.getToStep().equals( step ) && !hops.contains( hop ) ) {
          hops.add( hop );
          findUpstreamHops( hops, all, trans, hop.getFromStep() );
        }
      }
    }

    Trans getTrans( final TransMeta serviceTrans ) {
      return new Trans( serviceTrans );
    }

    @Override public List<Trans> getTransList() {
      return Collections.emptyList();
    }
  }

}
