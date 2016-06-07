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
package org.pentaho.di.trans.dataservice.clients;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.agilebi.modeler.models.annotations.CreateMeasure;
import org.pentaho.agilebi.modeler.models.annotations.ModelAnnotation;
import org.pentaho.agilebi.modeler.models.annotations.ModelAnnotationGroup;
import org.pentaho.agilebi.modeler.models.annotations.ModelAnnotationGroupXmlWriter;
import org.pentaho.di.core.KettleClientEnvironment;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.jdbc.ThinResultFactory;
import org.pentaho.di.trans.dataservice.jdbc.ThinResultSet;
import org.pentaho.di.trans.dataservice.serialization.DataServiceFactory;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.w3c.dom.Document;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AnnotationsQueryServiceTest {
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    if ( !KettleClientEnvironment.isInitialized() ) {
      KettleClientEnvironment.init();
    }
    PluginRegistry.addPluginType( StepPluginType.getInstance() );
    PluginRegistry.init();
    if ( !Props.isInitialized() ) {
      Props.init( 0 );
    }
  }

  @Test
  public void testLogsExceptionOnWriteEror() throws Exception {
    final DataServiceFactory dataServiceFactory = mock( DataServiceFactory.class );
    final AnnotationsQueryService queryService = new AnnotationsQueryService( dataServiceFactory );
    Query query =
      queryService.prepareQuery( "show annotations from annotatedService", 0, Collections.<String, String>emptyMap() );
    MetaStoreException metaStoreException = new MetaStoreException( "something happened" );
    when( dataServiceFactory.getDataService( "annotatedService" ) )
      .thenThrow( metaStoreException );
    final LogChannelInterface logChannel = mock( LogChannelInterface.class );
    when( dataServiceFactory.getLogChannel() ).thenReturn( logChannel );
    try {
      query.writeTo( new ByteArrayOutputStream(  ) );
      fail( "should have got exception" );
    } catch ( IOException e ) {
      assertEquals( "Error while executing 'show annotations from annotatedService'", e.getMessage() );
    }
    verify( logChannel )
      .logError( "Error while executing 'show annotations from annotatedService'", metaStoreException );
  }

  @Test
  public void testGetsAnnotationsDefinedByStream() throws Exception {
    final DataServiceFactory dataServiceFactory = mock( DataServiceFactory.class );
    final AnnotationsQueryService queryService = new AnnotationsQueryService( dataServiceFactory );

    URL resource = getClass().getClassLoader().getResource( "showAnnotations.ktr" );
    @SuppressWarnings( "ConstantConditions" )
    final TransMeta transMeta = new TransMeta( resource.getPath(), "show annotations" );
    Document document = XMLHandler.loadXMLFile( resource );
    transMeta.loadXML( document.getFirstChild(), null, true );
    final DataServiceMeta dataServiceMeta = new DataServiceMeta( transMeta );
    dataServiceMeta.setName( "annotatedService" );
    dataServiceMeta.setStepname( "Annotate Stream" );
    when( dataServiceFactory.getDataService( "annotatedService" ) ).thenReturn( dataServiceMeta );
    Query query =
      queryService.prepareQuery( "show annotations from annotatedService", 0, Collections.<String, String>emptyMap() );
    AnnotationsQueryService.AnnotationsQuery spy = (AnnotationsQueryService.AnnotationsQuery) Mockito.spy( query );
    final ModelAnnotationGroup mag = new ModelAnnotationGroup( new ModelAnnotation<>( new CreateMeasure() ) );
    when( spy.getTrans( transMeta ) ).then( new Answer<Trans>() {
      @Override public Trans answer( final InvocationOnMock invocationOnMock ) throws Throwable {
        Trans trans = (Trans) invocationOnMock.callRealMethod();
        trans.getExtensionDataMap().put( "KEY_MODEL_ANNOTATIONS", mag );
        return trans;
      }
    } );

    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    spy.writeTo( outputStream );
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream( outputStream.toByteArray() );
    DataInputStream dataInputStream = new DataInputStream( byteArrayInputStream );
    ThinResultSet thinResultSet = new ThinResultFactory().loadResultSet( dataInputStream, null );
    thinResultSet.next();
    String output = thinResultSet.getString( 1 );
    assertEquals( new ModelAnnotationGroupXmlWriter( mag ).getXML(), output );
    thinResultSet.close();
  }

  @Test
  public void testNoAnnotationsReturnsEmpty() throws Exception {
    final DataServiceFactory dataServiceFactory = mock( DataServiceFactory.class );
    final AnnotationsQueryService queryService = new AnnotationsQueryService( dataServiceFactory );

    URL resource = getClass().getClassLoader().getResource( "showAnnotations.ktr" );
    @SuppressWarnings( "ConstantConditions" )
    final TransMeta transMeta = new TransMeta( resource.getPath(), "show annotations" );
    Document document = XMLHandler.loadXMLFile( resource );
    transMeta.loadXML( document.getFirstChild(), null, true );
    final DataServiceMeta dataServiceMeta = new DataServiceMeta( transMeta );
    dataServiceMeta.setName( "gridService" );
    dataServiceMeta.setStepname( "Data Grid" );
    when( dataServiceFactory.getDataService( "gridService" ) ).thenReturn( dataServiceMeta );
    Query query =
      queryService.prepareQuery( "show annotations from gridService", 0, Collections.<String, String>emptyMap() );
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    query.writeTo( outputStream );
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream( outputStream.toByteArray() );
    DataInputStream dataInputStream = new DataInputStream( byteArrayInputStream );
    ThinResultSet thinResultSet = new ThinResultFactory().loadResultSet( dataInputStream, null );
    thinResultSet.next();
    String output = thinResultSet.getString( 1 );
    assertEquals( "<annotations></annotations>", output.trim() );
    thinResultSet.close();
  }
}
