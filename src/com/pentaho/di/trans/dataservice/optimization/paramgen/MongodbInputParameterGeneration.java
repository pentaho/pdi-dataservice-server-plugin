package com.pentaho.di.trans.dataservice.optimization.paramgen;


import com.pentaho.di.trans.dataservice.optimization.PushDownOptimizationException;
import com.pentaho.di.trans.dataservice.optimization.ValueMetaResolver;
import com.pentaho.di.trans.dataservice.optimization.mongod.MongodbPredicate;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.trans.step.StepInterface;

public class MongodbInputParameterGeneration implements ParameterGenerationService {

  protected final ValueMetaResolver valueMetaResolver;

  public MongodbInputParameterGeneration( ValueMetaResolver resolver ) {
    valueMetaResolver = resolver;
  }

  public MongodbInputParameterGeneration() {
    valueMetaResolver = new ValueMetaResolver( new RowMeta() );
  }

  @Override
  public void pushDown( Condition condition, ParameterGeneration parameterGeneration, StepInterface stepInterface ) throws PushDownOptimizationException {
    if ( !"MongoDbInput".equals( stepInterface.getStepMeta().getTypeId() ) ) {
      throw new PushDownOptimizationException( "Unable to push down to type " + stepInterface.getClass() );
    }
    stepInterface.setVariable( parameterGeneration.getParameterName(),
      getMongodbPredicate( condition ).asFilterCriteria() );
  }

  @Override
  public String getParameterDefault() {
    return "{_id:{$exists:true}}";
  }

  protected MongodbPredicate getMongodbPredicate( Condition condition ) {
    return new MongodbPredicate( condition, valueMetaResolver );
  }
}
