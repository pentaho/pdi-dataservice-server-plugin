/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package org.pentaho.di.trans.dataservice.clients;

import org.pentaho.agilebi.modeler.models.annotations.ModelAnnotationGroup;
import org.pentaho.agilebi.modeler.models.annotations.ModelAnnotationGroupXmlWriter;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.service.PluginServiceLoader;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;
import org.pentaho.di.trans.dataservice.resolvers.DataServiceResolver;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.locator.api.MetastoreLocator;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.pentaho.di.trans.dataservice.clients.TransMutators.disableAllUnrelatedHops;


public class AnnotationsQueryService implements Query.Service {
  private MetastoreLocator metastoreLocator;
  private DataServiceResolver resolver;


  public AnnotationsQueryService( final MetastoreLocator metastoreLocator, final DataServiceResolver resolver ) {
    this.metastoreLocator = metastoreLocator;
    this.resolver = resolver;
  }

  // OSGi blueprint constructor
  public AnnotationsQueryService( final DataServiceResolver resolver ) {
    this.resolver = resolver;
  }

  protected synchronized MetastoreLocator getMetastoreLocator() {
    if ( metastoreLocator == null ) {
      try {
        Collection<MetastoreLocator> metastoreLocators = PluginServiceLoader.loadServices( MetastoreLocator.class );
        metastoreLocator = metastoreLocators.stream().findFirst().get();
      } catch ( Exception e ) {
        LogChannel.GENERAL.logError( "Error getting MetastoreLocator", e );
        throw new IllegalStateException( e );
      }
    }
    return this.metastoreLocator;
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
      trans.setMetaStore( getMetastoreLocator().getMetastore() );
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
