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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URL;
import java.sql.SQLException;
import java.util.Collections;
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
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.jdbc.ThinResultFactory;
import org.pentaho.di.trans.dataservice.jdbc.ThinResultSet;
import org.pentaho.di.trans.dataservice.resolvers.DataServiceResolver;
import org.pentaho.di.trans.dataservice.ui.DataServiceDelegate;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.dummytrans.DummyTrans;
import org.pentaho.di.trans.steps.dummytrans.DummyTransMeta;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.locator.api.MetastoreLocator;
import org.w3c.dom.Document;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
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
    final DataServiceDelegate dataServiceFactory = mock( DataServiceDelegate.class );
    final DataServiceContext dataServiceContext = mock( DataServiceContext.class );
    final DataServiceResolver dataServiceResolver = mock( DataServiceResolver.class );
    final MetastoreLocator metastoreLocator = mock( MetastoreLocator.class );

    when( dataServiceContext.getDataServiceDelegate() ).thenReturn( dataServiceFactory );
    when( metastoreLocator.getMetastore() ).thenReturn( null );

    final AnnotationsQueryService queryService = new AnnotationsQueryService( metastoreLocator, dataServiceResolver );
    Query query =
      queryService.prepareQuery( "show annotations from annotatedService", 0, Collections.<String, String>emptyMap() );
    MetaStoreException metaStoreException = new MetaStoreException( "Unable to load dataservice annotatedService" );
    when( dataServiceResolver.getDataService( "annotatedService" ) ).thenReturn( null );

    try {
      query.writeTo( new ByteArrayOutputStream(  ) );
      fail( "should have got exception" );
    } catch ( IOException e ) {
      assertEquals( "Error while executing 'show annotations from annotatedService'", e.getMessage() );
    }
  }

  @Test
  public void testGetsAnnotationsDefinedByStream() throws Exception {
    final DataServiceDelegate dataServiceFactory = mock( DataServiceDelegate.class );
    final DataServiceContext dataServiceContext = mock( DataServiceContext.class );
    final DataServiceResolver dataServiceResolver = mock( DataServiceResolver.class );
    final MetastoreLocator metastoreLocator = mock( MetastoreLocator.class );

    when( dataServiceContext.getDataServiceDelegate() ).thenReturn( dataServiceFactory );
    when( metastoreLocator.getMetastore() ).thenReturn( null );
    final AnnotationsQueryService queryService = new AnnotationsQueryService( metastoreLocator, dataServiceResolver );

    URL resource = getClass().getClassLoader().getResource( "showAnnotations.ktr" );
    @SuppressWarnings( "ConstantConditions" )
    final TransMeta transMeta = new TransMeta( resource.getPath(), "show annotations" );
    Document document = XMLHandler.loadXMLFile( resource );
    transMeta.loadXML( document.getFirstChild(), null, true );
    final DataServiceMeta dataServiceMeta = new DataServiceMeta( transMeta );
    dataServiceMeta.setName( "annotatedService" );
    dataServiceMeta.setStepname( "Annotate Stream" );
    when( dataServiceResolver.getDataService( "annotatedService" ) ).thenReturn( dataServiceMeta );
    Query query =
      queryService.prepareQuery( "show annotations from annotatedService", 0, Collections.<String, String>emptyMap() );
    AnnotationsQueryService.AnnotationsQuery spy = (AnnotationsQueryService.AnnotationsQuery) Mockito.spy( query );
    final ModelAnnotationGroup mag = new ModelAnnotationGroup( new ModelAnnotation<>( new CreateMeasure() ) );
    final Trans[] transSpy = { null };
    when( spy.getTrans( transMeta ) ).then( new Answer<Trans>() {
      @Override public Trans answer( final InvocationOnMock invocationOnMock ) throws Throwable {
        Trans trans = (Trans) invocationOnMock.callRealMethod();
        // Keep this reference to later validate its use
        transSpy[ 0 ] = Mockito.spy( trans );
        trans.getExtensionDataMap().put( "KEY_MODEL_ANNOTATIONS", mag );
        return transSpy[ 0 ];
      }
    } );

    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    spy.writeTo( outputStream );
    // PDI-18214/CDA-243: The disposal of the steps must be done
    verify( transSpy[ 0 ] ).cleanup();
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream( outputStream.toByteArray() );
    DataInputStream dataInputStream = new DataInputStream( byteArrayInputStream );
    ThinResultSet thinResultSet = new ThinResultFactory().loadResultSet( dataInputStream, null );
    thinResultSet.next();
    String output = thinResultSet.getString( 1 );
    assertEquals( new ModelAnnotationGroupXmlWriter( mag ).getXML().trim(), output );
    verify( metastoreLocator ).getMetastore();
    assertEquals( 0, query.getTransList().size() );
    thinResultSet.close();
  }

  @Test
  public void testNoAnnotationsReturnsEmpty() throws Exception {
    final DataServiceDelegate dataServiceFactory = mock( DataServiceDelegate.class );
    final DataServiceContext dataServiceContext = mock( DataServiceContext.class );
    final DataServiceResolver dataServiceResolver = mock( DataServiceResolver.class );
    final MetastoreLocator metastoreLocator = mock( MetastoreLocator.class );

    when( dataServiceContext.getDataServiceDelegate() ).thenReturn( dataServiceFactory );
    when( metastoreLocator.getMetastore() ).thenReturn( null );
    final AnnotationsQueryService queryService = new AnnotationsQueryService( metastoreLocator, dataServiceResolver );


    URL resource = getClass().getClassLoader().getResource( "showAnnotations.ktr" );
    @SuppressWarnings( "ConstantConditions" )
    final TransMeta transMeta = new TransMeta( resource.getPath(), "show annotations" );
    Document document = XMLHandler.loadXMLFile( resource );
    transMeta.loadXML( document.getFirstChild(), null, true );
    final DataServiceMeta dataServiceMeta = new DataServiceMeta( transMeta );
    dataServiceMeta.setName( "gridService" );
    dataServiceMeta.setStepname( "Data Grid" );
    when( dataServiceResolver.getDataService( "gridService" ) ).thenReturn( dataServiceMeta );
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

    final DataServiceDelegate dataServiceFactory = mock( DataServiceDelegate.class );
    final DataServiceContext dataServiceContext = mock( DataServiceContext.class );
    when( dataServiceContext.getDataServiceDelegate() ).thenReturn( dataServiceFactory );
    final DataServiceResolver dataServiceResolver = mock( DataServiceResolver.class );

    when( dataServiceResolver.getDataService( "ds" ) ).thenReturn( ds1 );
    when( dataServiceResolver.getDataService( "dsa" ) ).thenReturn( dsA );

    final MetastoreLocator metastoreLocator = mock( MetastoreLocator.class );

    when( metastoreLocator.getMetastore() ).thenReturn( null );
    final AnnotationsQueryService queryService = new AnnotationsQueryService( metastoreLocator, dataServiceResolver );

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

    final DataServiceDelegate dataServiceFactory = mock( DataServiceDelegate.class );
    final DataServiceResolver dataServiceResolver = mock( DataServiceResolver.class );
    when( dataServiceResolver.getDataService( "dsAll" ) ).thenReturn( dsAll );
    when( dataServiceResolver.getDataService( "ds1" ) ).thenReturn( ds1 );

    final DataServiceContext dataServiceContext = mock( DataServiceContext.class );
    when( dataServiceContext.getDataServiceDelegate() ).thenReturn( dataServiceFactory );
    final MetastoreLocator metastoreLocator = mock( MetastoreLocator.class );

    when( metastoreLocator.getMetastore() ).thenReturn( null );
    AnnotationsQueryService queryService = new AnnotationsQueryService( metastoreLocator, dataServiceResolver );

    Query query =
        queryService.prepareQuery( "show annotations from ds1", 0, Collections.<String, String>emptyMap() );
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    query.writeTo( outputStream );
    String result = getResultString( outputStream );
    assertTrue( result.contains( name1 ) );
    assertFalse( result.contains( name2 ) );

    queryService = new AnnotationsQueryService( metastoreLocator, dataServiceResolver );
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
