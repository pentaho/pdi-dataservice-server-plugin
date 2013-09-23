/*!
* Copyright 2010 - 2013 Pentaho Corporation.  All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
*/

package com.pentaho.di.trans.step;

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
    while (!rs.putRow(rowMeta, row) && !step.isStopped()) {
      // Wait
    }
  }

  @Override
  public EImage getDistributionImage() {
    return EImage.LOAD_BALANCE;
  }

}
