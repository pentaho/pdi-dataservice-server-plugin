package org.pentaho.di.trans.dataservice;

import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.trans.dataservice.serialization.DataServiceMetaStoreUtil;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

/**
 * @author nhudak
 */

@RunWith( org.mockito.runners.MockitoJUnitRunner.class )
public class DataServiceContextTest extends BaseTest {
  @Before
  public void setUp() throws Exception {
    context = new DataServiceContext(
      pushDownFactories, autoOptimizationServices,
      cacheManager, uiFactory, logChannel
    );

    assertThat( context.getCacheManager(), sameInstance( cacheManager ) );
    assertThat( context.getUIFactory(), sameInstance( uiFactory ) );
    assertThat( context.getLogChannel(), sameInstance( logChannel ) );
    assertThat( context.getPushDownFactories(), sameInstance( pushDownFactories ) );
    assertThat( context.getAutoOptimizationServices(), sameInstance( autoOptimizationServices ) );
  }

  @Test
  public void testGetMetaStoreUtil() throws Exception {
    assertThat( context.getMetaStoreUtil(), validMetaStoreUtil() );
  }

  @Test
  public void testCreateBuilder() throws Exception {
    assertThat( context.createBuilder( mock( SQL.class ), dataService ), notNullValue() );
  }

  @Test
  public void testGetDataServiceClient() throws Exception {
    assertThat( context.getDataServiceClient(), notNullValue() );
  }

  @Test
  public void testGetDataServiceDelegate() throws Exception {
    assertThat( context.getDataServiceDelegate(), validMetaStoreUtil() );
  }

  protected Matcher<DataServiceMetaStoreUtil> validMetaStoreUtil() {
    return allOf(
      hasProperty( "context", sameInstance( context ) ),
      hasProperty( "stepCache", sameInstance( cache ) ),
      hasProperty( "logChannel", sameInstance( logChannel ) ),
      hasProperty( "pushDownFactories", sameInstance( pushDownFactories ) )
    );
  }
}
