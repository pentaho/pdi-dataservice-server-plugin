package org.pentaho.di.trans.step;

import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.gui.PrimitiveGCInterface.EImage;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.RowDistributionInterface;
import org.pentaho.di.trans.step.RowDistributionPlugin;
import org.pentaho.di.trans.step.StepInterface;

@RowDistributionPlugin(
    code="LoadBalance",
    name="Load balance",
    description="Tries to send rows to any output row set which is not full"
    )
public class LoadBalancingRowDistribution implements RowDistributionInterface {

  @Override
  public String getCode() {
    return "LoadBalance";
  }

  @Override
  public String getDescription() {
    return "Load balance";
  }

  @Override
  public void distributeRow(RowMetaInterface rowMeta, Object[] row, StepInterface step) throws KettleStepException {
    // LOAD BALANCE:
    // ----------------
    //
    // To balance the output, look for the most empty output buffer and take that one to write to.
    //
    int smallestSize = Integer.MAX_VALUE;
    for (int i = 0; i < step.getOutputRowSets().size(); i++) {
      RowSet candidate = step.getOutputRowSets().get(i);
      if (candidate.size() < smallestSize) {
        step.setCurrentOutputRowSetNr(i);
        smallestSize = candidate.size();
      }
    }
    RowSet rs = step.getOutputRowSets().get(step.getCurrentOutputRowSetNr());

    // Loop until we find room in the target rowset, could very well all be full so keep trying.
    //
    while (!rs.putRow(rowMeta, row) && !step.isStopped())
    ;
  }

  @Override
  public EImage getDistributionImage() {
    return EImage.LOAD_BALANCE;
  }

}
