/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2023 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.optimization.cache;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.core.sql.SQLCondition;
import org.pentaho.di.core.sql.SQLField;
import org.pentaho.di.core.sql.SQLFields;
import org.pentaho.di.core.sql.SQLLimit;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;

import java.io.Serializable;
import java.util.List;

/**
 * @author nhudak
 */
class CachedService implements Serializable {

  private static final long serialVersionUID = 1L;
  private final ImmutableList<RowMetaAndData> rowMetaAndData;
  private final Optional<Integer> ranking;
  private final Optional<Integer> rankingServiceRows;

  private CachedService( List<RowMetaAndData> rowMetaAndData, Optional<Integer> ranking,  Optional<Integer> ranking2 ) {
    this.rowMetaAndData = ImmutableList.copyOf( rowMetaAndData );
    this.ranking = ranking;
    this.rankingServiceRows = ranking2;
  }

  public static CachedService complete( List<RowMetaAndData> rowMetaAndData ) {
    // Key based on service name and where clause only. Ordering here does not matter
    return new CachedService(
      rowMetaAndData,
      Optional.<Integer>absent(),
      Optional.<Integer>absent()
    );
  }

  public static CachedService partial( List<RowMetaAndData> rowMetaAndData, DataServiceExecutor executor ) {
    return new CachedService(
      rowMetaAndData,
      Optional.of( calculateRank( executor ) ),
      Optional.of( calculateServiceRowRank( executor ) )
    );
  }

  public List<RowMetaAndData> getRowMetaAndData() {
    return rowMetaAndData;
  }

  public Optional<Integer> getRanking() {
    return ranking;
  }

  @Override public String toString() {
    return MoreObjects.toStringHelper( this )
      .add( "rowMetaAndData.length", rowMetaAndData.size() )
      .add( "ranking", ranking )
      .toString();
  }

  public boolean isComplete() {
    return !ranking.isPresent() && !rankingServiceRows.isPresent();
  }

  /**
   * Calculates a ranking based on the number of rows consumed by this query.
   * A higher ranking denotes a more complete result set, up to {@link Integer#MAX_VALUE}.
   *
   * @param executor Executor to be ranked
   * @return Value denoting completeness of a query's result set
   */
  static int calculateRank( DataServiceExecutor executor ) {
    SQLLimit limitValues = executor.getSql().getLimitValues();
    if ( limitValues != null ) {
      return limitValues.getLimit() + limitValues.getOffset();
    }
    if ( executor.getRowLimit() > 0 ) {
      return executor.getRowLimit();
    }
    return Integer.MAX_VALUE;
  }

  private static int calculateServiceRowRank( DataServiceExecutor executor ) {
    if ( executor.getServiceRowLimit() > 0 ) {
      return executor.getServiceRowLimit();
    }
    return Integer.MAX_VALUE;
  }

  public boolean answersQuery( DataServiceExecutor executor ) {
    SQL sql = executor.getSql();
    // If this loader is complete, it always answers the query
    if ( isComplete() ) {
      return true;
    }
    // If aggregate functions or grouping is queried, a complete set is needed
    SQLFields selectFields = sql.getSelectFields();
    SQLFields groupFields = sql.getGroupFields();
    if ( selectFields.hasAggregates() || selectFields.isDistinct() || !groupFields.getFields().isEmpty() ) {
      if ( ranking.or( Integer.MAX_VALUE ) < Integer.MAX_VALUE ) {
        return false;
      }
    }
    boolean outRanks = true;
    if ( this.ranking.isPresent() ) {
      outRanks &= this.ranking.get() >= calculateRank( executor );
    }
    // the two row limits my or may not be related, treating as independent
    if ( this.rankingServiceRows.isPresent() ) {
      outRanks &= this.rankingServiceRows.get() >= calculateServiceRowRank( executor );
    }
    return outRanks;
  }

  public static final class CacheKey implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Required
     */
    private final int transMetaVersionId;
    /**
     * Required
     */
    private final ImmutableMap<String, String> parameters;
    /**
     * Optional
     */
    private final Optional<String> whereClause;
    /**
     * Optional
     */
    private final ImmutableList<String> orderByFields;

    private CacheKey( int transMetaVersionId, ImmutableMap<String, String> parameters, Optional<String> whereClause,
                      ImmutableList<String> orderByFields ) {
      this.transMetaVersionId = transMetaVersionId;
      this.parameters = parameters;
      this.whereClause = whereClause;
      this.orderByFields = orderByFields;
    }

    public static CacheKey create( DataServiceExecutor executor ) {
      SQL sql = executor.getSql();

      // Calculate trans meta version. If the service transformation changes, keys will no longer match
      int version;
      // Get trans meta from service instead of executor, since executor modifies the meta with every query
      TransMeta trans = executor.getService().getServiceTrans();
      try {
        version = trans.getCacheVersion();
      } catch ( KettleException e ) {
        throw new IllegalStateException( "Unable to determine version of transMeta", e );
      }

      // Extract where condition
      Optional<String> whereClause = Optional.fromNullable( sql.getWhereCondition() ).transform(
        // Simplify  and rewrite condition, more likely to match future queries
        new Function<SQLCondition, String>() {
          @Override public String apply( SQLCondition input ) {
            Condition clone = (Condition) input.getCondition().clone();
            clone.simplify();
            return clone.toString();
          }
        }
      );

      // Extract ORDER BY fields from SQL
      ImmutableList<String> orderByFields = FluentIterable
        .from( Optional.fromNullable( sql.getOrderFields() ).asSet() )
        .transformAndConcat(
          new Function<SQLFields, Iterable<SQLField>>() {
            @Override public Iterable<SQLField> apply( SQLFields input ) {
              return input.getFields();
            }
          }
        )
        .transform(
          new Function<SQLField, String>() {
            @Override public String apply( SQLField input ) {
              return input.getField();
            }
          }
        )
        .toList();

      // Copy execution parameters
      ImmutableMap<String, String> parameters = ImmutableMap.copyOf( executor.getParameters() );

      return new CacheKey( version, parameters, whereClause, orderByFields );
    }

    /**
     * <p>
     * Generate a ordered set of this keys for this query, ranging from most specific to generic.
     * </p>
     * <p>
     * Any returned cache entry may be able to replay the transformation,
     * but should be tested with {@link #answersQuery}
     * </p>
     * Accounts for field grouping, WHERE condition, and data service.
     *
     * @return ordered set of CacheKeys
     */
    public ImmutableSet<CacheKey> all() {
      // Immutable Sets always return elements in the specified order.
      return ImmutableSet.of(
        this,
        this.withoutOrder(),
        this.withoutOrder().withoutCondition()
      );
    }

    public CacheKey withoutCondition() {
      return new CacheKey( transMetaVersionId, parameters, Optional.<String>absent(), orderByFields );
    }

    public CacheKey withoutOrder() {
      return new CacheKey( transMetaVersionId, parameters, whereClause, ImmutableList.<String>of() );
    }

    @Override public boolean equals( Object o ) {
      if ( this == o ) {
        return true;
      }
      if ( o == null || getClass() != o.getClass() ) {
        return false;
      }
      CacheKey cacheKey = (CacheKey) o;
      return Objects.equal( transMetaVersionId, cacheKey.transMetaVersionId )
          && Objects.equal( parameters, cacheKey.parameters )
          && Objects.equal( whereClause, cacheKey.whereClause )
          && Objects.equal( orderByFields, cacheKey.orderByFields );
    }

    @Override public int hashCode() {
      return Objects.hashCode( transMetaVersionId, parameters, whereClause, orderByFields );
    }

    @Override public String toString() {
      return MoreObjects.toStringHelper( CacheKey.class )
        .add( "transMetaVersionId", transMetaVersionId )
        .add( "parameters", parameters )
        .add( "whereClause", whereClause )
        .add( "orderByFields", orderByFields )
        .toString();
    }
  }

}
