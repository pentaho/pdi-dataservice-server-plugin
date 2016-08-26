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
import org.pentaho.agilebi.modeler.models.annotations.CreateAttribute;
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
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.jdbc.ThinResultFactory;
import org.pentaho.di.trans.dataservice.jdbc.ThinResultSet;
import org.pentaho.di.trans.dataservice.serialization.DataServiceFactory;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.dummytrans.DummyTrans;
import org.pentaho.di.trans.steps.dummytrans.DummyTransMeta;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.w3c.dom.Document;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URL;
import java.sql.SQLException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
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
    verify( dataServiceFactory ).getMetaStore();
    assertEquals( 0, query.getTransList().size() );
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
    String output = getResultString( outputStream );
    assertEquals( "<annotations></annotations>", output.trim() );

  }

  private String getResultString( final ByteArrayOutputStream outputStream ) throws SQLException {
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream( outputStream.toByteArray() );
    DataInputStream dataInputStream = new DataInputStream( byteArrayInputStream );
    ThinResultSet thinResultSet = new ThinResultFactory().loadResultSet( dataInputStream, null );
    thinResultSet.next();
    String output = thinResultSet.getString( 1 );
    thinResultSet.close();
    return output;
  }

  @Test
  public void testAnnotationsOnCurrentStep() throws Exception {
    TransMeta transMeta = new TransMeta();

    DummyTransMeta src1Meta = new DummyTransMeta();
    StepMeta src1 = new StepMeta( "src", src1Meta );
    transMeta.addStep( src1 );

    final ModelAnnotationGroup mag1 =
        new ModelAnnotationGroup( new ModelAnnotation<CreateAttribute>( new CreateAttribute() ) );
    final String name1 = mag1.get( 0 ).getName();
    DummyTransMeta annot1Meta = createPseudoAnnotate( mag1 );
    StepMeta annot1 = Mockito.spy( new StepMeta( "annot", annot1Meta ) );
    transMeta.addStep( annot1 );

    TransHopMeta src1ToAnnot1 = new TransHopMeta( src1, annot1 );
    transMeta.addTransHop( src1ToAnnot1 );

    final DataServiceMeta dsA = new DataServiceMeta( transMeta );
    dsA.setName( "dsa" );
    dsA.setStepname( "annot" );
    final DataServiceMeta ds1 = new DataServiceMeta( transMeta );
    ds1.setName( "ds" );
    ds1.setStepname( "src" );

    final DataServiceFactory dataServiceFactory = mock( DataServiceFactory.class );
    when( dataServiceFactory.getDataService( "ds" ) ).thenReturn( ds1 );
    when( dataServiceFactory.getDataService( "dsa" ) ).thenReturn( dsA );

    final AnnotationsQueryService queryService = new AnnotationsQueryService( dataServiceFactory );
    Query query =
        queryService.prepareQuery( "show annotations from dsa", 0, Collections.<String, String>emptyMap() );
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    query.writeTo( outputStream );
    String result = getResultString( outputStream );
    assertTrue( result.contains( name1 ) );

    query =
        queryService.prepareQuery( "show annotations from ds", 0, Collections.<String, String>emptyMap() );
    outputStream = new ByteArrayOutputStream();
    query.writeTo( outputStream );
    result = getResultString( outputStream );
    String output = getResultString( outputStream );
    assertEquals( "<annotations></annotations>", output.trim() );
  }

  @Test
  public void testAnnotationsOnCurrentStep2Paths() throws Exception {
    TransMeta transMeta = new TransMeta();

    DummyTransMeta src1Meta = new DummyTransMeta();
    StepMeta src1 = new StepMeta( "src1", src1Meta );
    transMeta.addStep( src1 );

    final ModelAnnotationGroup mag1 =
        new ModelAnnotationGroup( new ModelAnnotation<CreateAttribute>( new CreateAttribute() ) );
    final String name1 = mag1.get( 0 ).getName();
    DummyTransMeta annot1Meta = createPseudoAnnotate( mag1 );
    StepMeta annot1 = new StepMeta( "annot1", annot1Meta );
    transMeta.addStep( annot1 );

    TransHopMeta src1ToAnnot1 = new TransHopMeta( src1, annot1 );
    transMeta.addTransHop( src1ToAnnot1 );

    DummyTransMeta src2Meta = new DummyTransMeta();
    StepMeta src2 = new StepMeta( "src2", src2Meta );
    transMeta.addStep( src2 );

    final ModelAnnotationGroup mag2 =
        new ModelAnnotationGroup( new ModelAnnotation<CreateAttribute>( new CreateAttribute() ) );
    final String name2 = mag2.get( 0 ).getName();
    DummyTransMeta annot2Meta = createPseudoAnnotate( mag2 );
    StepMeta annot2 = new StepMeta( "annot2", annot2Meta );
    transMeta.addStep( annot2 );

    TransHopMeta src2ToAnnot2 = new TransHopMeta( src2, annot2 );
    transMeta.addTransHop( src2ToAnnot2 );

    DummyTransMeta mergedMeta = new DummyTransMeta();
    StepMeta mergedStepMeta = new StepMeta( "merged", mergedMeta );
    transMeta.addStep( mergedStepMeta );

    transMeta.addTransHop( new TransHopMeta( annot1, mergedStepMeta ) );
    transMeta.addTransHop( new TransHopMeta( annot2, mergedStepMeta ) );

    final DataServiceMeta ds1 = new DataServiceMeta( (TransMeta) transMeta.clone() );
    ds1.setName( "dsa" );
    ds1.setStepname( "annot1" );
    final DataServiceMeta dsAll = new DataServiceMeta( (TransMeta) transMeta.clone() );
    dsAll.setName( "ds" );
    dsAll.setStepname( "merged" );

    final DataServiceFactory dataServiceFactory = mock( DataServiceFactory.class );
    when( dataServiceFactory.getDataService( "dsAll" ) ).thenReturn( dsAll );
    when( dataServiceFactory.getDataService( "ds1" ) ).thenReturn( ds1 );

    AnnotationsQueryService queryService = new AnnotationsQueryService( dataServiceFactory );
    Query query =
        queryService.prepareQuery( "show annotations from ds1", 0, Collections.<String, String>emptyMap() );
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    query.writeTo( outputStream );
    String result = getResultString( outputStream );
    assertTrue( result.contains( name1 ) );
    assertFalse( result.contains( name2 ) );

    queryService = new AnnotationsQueryService( dataServiceFactory );
    query =
        queryService.prepareQuery( "show annotations from dsAll", 0, Collections.<String, String>emptyMap() );
    outputStream = new ByteArrayOutputStream();
    query.writeTo( outputStream );
    String result2 = getResultString( outputStream );
    assertTrue( result2.contains( name1 ) );
    assertTrue( result2.contains( name2 ) );
  }

  private DummyTransMeta createPseudoAnnotate( final ModelAnnotationGroup mag ) {
    final String magicKey = "KEY_MODEL_ANNOTATIONS";
    DummyTransMeta annot1Meta = new DummyTransMeta() {
      @Override
      public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
          TransMeta tr, final Trans trans ) {
        return new DummyTrans( stepMeta, stepDataInterface, cnr, tr, trans ) {
          @Override
          public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
            ModelAnnotationGroup existing = (ModelAnnotationGroup) trans.getExtensionDataMap().get( magicKey );
            if ( existing == null ) {
              trans.getExtensionDataMap().put( magicKey, mag );
            } else {
              existing.addAll( mag );
            }
            return true;
          }
        };
      }
    };
    return annot1Meta;
  }
}
