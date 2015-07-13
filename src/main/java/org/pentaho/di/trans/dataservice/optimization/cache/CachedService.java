/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2015 Pentaho Corporation (Pentaho). All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Pentaho and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Pentaho and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Pentaho is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Pentaho,
 * explicitly covering such access.
 */

package org.pentaho.di.trans.dataservice.optimization.cache;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.core.sql.SQLCondition;
import org.pentaho.di.core.sql.SQLField;
import org.pentaho.di.core.sql.SQLFields;
import org.pentaho.di.core.sql.SQLLimit;

import java.io.Serializable;
import java.util.List;

/**
 * @author nhudak
 */
class CachedService implements Serializable {
  private final ImmutableList<RowMetaAndData> rowMetaAndData;
  private final Optional<Integer> ranking;

  private CachedService( List<RowMetaAndData> rowMetaAndData, Optional<Integer> ranking ) {
    this.rowMetaAndData = ImmutableList.copyOf( rowMetaAndData );
    this.ranking = ranking;
  }

  public static CachedService complete( List<RowMetaAndData> rowMetaAndData ) {
    // Key based on service name and where clause only. Ordering here does not matter
    return new CachedService(
      rowMetaAndData,
      Optional.<Integer>absent()
    );
  }

  public static CachedService partial( List<RowMetaAndData> rowMetaAndData, DataServiceExecutor executor ) {
    return new CachedService(
      rowMetaAndData,
      Optional.of( calculateRank( executor ) )
    );
  }

  public List<RowMetaAndData> getRowMetaAndData() {
    return rowMetaAndData;
  }

  public Optional<Integer> getRanking() {
    return ranking;
  }

  @Override public String toString() {
    return Objects.toStringHelper( this )
      .add( "rowMetaAndData.length", rowMetaAndData.size() )
      .add( "ranking", ranking )
      .toString();
  }

  public boolean isComplete() {
    return !ranking.isPresent();
  }

  /**
   * Calculates a ranking based on the number of rows consumed by this query.
   * A higher ranking denotes a more complete result set, up to {@link Integer#MAX_VALUE}.
   *
   * @param executor Executor to be ranked
   * @return Value denoting completeness of a query's result set
   */
  private static int calculateRank( DataServiceExecutor executor ) {
    SQLLimit limitValues = executor.getSql().getLimitValues();
    if ( limitValues != null ) {
      return limitValues.getLimit() + limitValues.getOffset();
    }
    if ( executor.getRowLimit() > 0 ) {
      return executor.getRowLimit();
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
    if ( !sql.getGroupFields().getFields().isEmpty() || !sql.getSelectFields().getAggregateFields().isEmpty() ) {
      return false;
    }
    // Compare ranking
    return this.ranking.get() >= calculateRank( executor );
  }

  public static final class CacheKey implements Serializable {
    /**
     * Required
     */
    private final String dataServiceName;
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

    private CacheKey( String dataServiceName, ImmutableMap<String, String> parameters, Optional<String> whereClause,
                      ImmutableList<String> orderByFields ) {
      this.dataServiceName = dataServiceName;
      this.parameters = parameters;
      this.whereClause = whereClause;
      this.orderByFields = orderByFields;
    }

    public static CacheKey create( DataServiceExecutor executor ) {
      SQL sql = executor.getSql();

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

      return new CacheKey( sql.getServiceName(), parameters, whereClause, orderByFields );
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
      return new CacheKey( dataServiceName, parameters, Optional.<String>absent(), orderByFields );
    }

    public CacheKey withoutOrder() {
      return new CacheKey( dataServiceName, parameters, whereClause, ImmutableList.<String>of() );
    }

    @Override public boolean equals( Object o ) {
      if ( this == o ) {
        return true;
      }
      if ( o == null || getClass() != o.getClass() ) {
        return false;
      }
      CacheKey cacheKey = (CacheKey) o;
      return Objects.equal( dataServiceName, cacheKey.dataServiceName ) &&
        Objects.equal( parameters, cacheKey.parameters ) &&
        Objects.equal( whereClause, cacheKey.whereClause ) &&
        Objects.equal( orderByFields, cacheKey.orderByFields );
    }

    @Override public int hashCode() {
      return Objects.hashCode( dataServiceName, whereClause, orderByFields );
    }

    @Override public String toString() {
      return Objects.toStringHelper( CacheKey.class )
        .add( "dataServiceName", dataServiceName )
        .add( "parameters", parameters )
        .add( "whereClause", whereClause )
        .add( "orderByFields", orderByFields )
        .toString();
    }
  }

}
