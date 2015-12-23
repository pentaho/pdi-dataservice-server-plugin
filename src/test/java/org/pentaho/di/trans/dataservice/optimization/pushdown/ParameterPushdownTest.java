/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.optimization.pushdown;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.value.ValueMetaBoolean;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.optimization.OptimizationImpactInfo;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */

@RunWith( org.mockito.runners.MockitoJUnitRunner.class )
public class ParameterPushdownTest {
  private static final String SELECT = "SELECT * FROM 'music' ";
  @Mock ParameterPushdownFactory factory;
  @Mock TransMeta transMeta;
  @Mock DataServiceExecutor executor;

  @InjectMocks ParameterPushdown parameterPushdown;
  private PushDownOptimizationMeta optimizationMeta;
  private DataServiceMeta dataService;
  private Map<String, String> parameters;
  private RowMeta rowMeta;

  @Before
  public void setUp() throws Exception {
    optimizationMeta = new PushDownOptimizationMeta();
    optimizationMeta.setType( parameterPushdown );
    optimizationMeta.setStepName( "OUTPUT" );

    dataService = new DataServiceMeta( transMeta );
    dataService.setStepname( "OUTPUT" );
    dataService.getPushDownOptimizationMeta().add( optimizationMeta );

    parameterPushdown.createDefinition()
      .setFieldName( "ARTIST" )
      .setParameter( "ARTIST_FIELD" )
      .setFormat( "artist: \"%s\"," );

    parameterPushdown.createDefinition()
      .setFieldName( "ALBUM" )
      .setParameter( "ALBUM_FIELD" )
      .setFormat( "album: \"%s\"," );

    parameterPushdown.createDefinition()
      .setFieldName( "LIVE" )
      .setParameter( "LIVE_FIELD" );

    when( executor.getServiceTransMeta() ).thenReturn( transMeta );
    when( executor.getParameters() ).thenReturn( parameters = Maps.newHashMap() );

    rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "ARTIST" ) );
    rowMeta.addValueMeta( new ValueMetaString( "ALBUM" ) );
    rowMeta.addValueMeta( new ValueMetaBoolean( "LIVE" ) );
  }

  @Test
  public void testInit() throws Exception {
    List<ParameterPushdown.Definition> orig = ImmutableList.copyOf( parameterPushdown.getDefinitions() );

    // Create some invalid definitions
    parameterPushdown.createDefinition();
    parameterPushdown.createDefinition().setFieldName( "field only" );
    parameterPushdown.createDefinition().setParameter( "param only" );

    assertThat( parameterPushdown.getDefinitions(), hasSize( 6 ) );

    parameterPushdown.init( transMeta, dataService, optimizationMeta );

    assertThat( optimizationMeta.getStepName(), is( "OUTPUT" ) );

    // Only the last should remain
    assertThat( parameterPushdown.getDefinitions(), equalTo( orig ) );
    ArgumentCaptor<String> paramCaptor = ArgumentCaptor.forClass( String.class );
    verify( transMeta, times( 3 ) ).addParameterDefinition( paramCaptor.capture(), eq( "" ), anyString() );
    assertThat( paramCaptor.getAllValues(), contains( "ARTIST_FIELD", "ALBUM_FIELD", "LIVE_FIELD" ) );
    verify( transMeta ).activateParameters();
  }

  @Test
  public void testFormat() throws Exception {
    ParameterPushdown.Definition definition = parameterPushdown.createDefinition()
      .setFieldName( "field" )
      .setParameter( "param" );

    assertThat( definition.getFormat(), is( ParameterPushdown.DEFAULT_FORMAT ) );
    definition.setFormat( "prefix: %s" );
    assertThat( definition.getFormat(), is( "prefix: %s" ) );
  }

  @Test
  public void testActivate() throws Exception {
    query( "WHERE ALBUM IN ('Thick as a Brick', 'Aqualung')" );
    assertThat( parameterPushdown.activate( executor, optimizationMeta ).get( 5, TimeUnit.SECONDS ), is( false ) );
    verifyActivate( Collections.<String, String>emptyMap() );

    query( "WHERE ( ARTIST = 'The Doors' AND ALBUM = 'Morrison Hotel' ) OR "
      + "( ARTIST = 'The Kinks' AND ALBUM = 'Lola Versus Powerman and the Money-Go-Round, Pt. 1' )" );
    verifyActivate( Collections.<String, String>emptyMap() );

    query( "WHERE ARTIST = 'Queen' OR ALBUM LIKE '% at the %' " );
    verifyActivate( Collections.<String, String>emptyMap() );

    query( "WHERE ALBUM = ARTIST" );
    verifyActivate( Collections.<String, String>emptyMap() );

    query( "WHERE ARTIST = 'The Who' AND ( ALBUM IN ( 'Tommy', 'Quadrophenia' ) OR LIVE = true )" );
    verifyActivate( ImmutableMap.of(
      "ARTIST_FIELD", "artist: \"The Who\","
    ) );

    query( "WHERE ARTIST = 'Pink Floyd' AND ALBUM = 'Atom Heart Mother' AND LIVE = true" );
    verifyActivate( ImmutableMap.of(
      "ARTIST_FIELD", "artist: \"Pink Floyd\",",
      "ALBUM_FIELD", "album: \"Atom Heart Mother\",",
      "LIVE_FIELD", "true"
    ) );

    query( "WHERE ARTIST IN ('Led Zeppelin', 'Deep Purple') AND LIVE = true" );
    verifyActivate( ImmutableMap.of(
      "LIVE_FIELD", "true"
    ) );
  }

  @Test
  public void testPreview() throws Exception {
    query( "WHERE ARTIST = 'The Rolling Stones' AND ALBUM = 'Let it Bleed' " );
    when( transMeta.getParameterDefault( anyString() ) ).thenReturn( "<default>" );

    OptimizationImpactInfo preview = parameterPushdown.preview( executor, optimizationMeta );
    assertThat( preview.getErrorMsg(), emptyOrNullString() );
    assertThat( preview.isModified(), is( true ) );
    assertThat( preview.getQueryBeforeOptimization(), equalTo(
      "ARTIST_FIELD = <default>\n" +
        "ALBUM_FIELD = <default>\n" +
        "LIVE_FIELD = <default>"
    ) );

    assertThat( preview.getQueryAfterOptimization(), equalTo(
      "ARTIST_FIELD = artist: \"The Rolling Stones\",\n" +
        "ALBUM_FIELD = album: \"Let it Bleed\",\n" +
        "LIVE_FIELD = <default>"
    ) );
  }

  private void query( String whereClause ) throws Exception {
    SQL sql = new SQL( SELECT + whereClause );
    sql.parse( rowMeta );
    when( executor.getSql() ).thenReturn( sql );
  }

  private void verifyActivate( Map<String, String> parameters ) throws Exception {
    Future<Boolean> future = parameterPushdown.activate( executor, optimizationMeta );
    assertThat( this.parameters, equalTo( parameters ) );
    assertThat( future.get( 5, TimeUnit.SECONDS ), equalTo( !parameters.isEmpty() ) );
    this.parameters.clear();
  }
}
