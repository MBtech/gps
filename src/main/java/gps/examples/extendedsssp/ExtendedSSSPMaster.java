package gps.examples.extendedsssp;

import java.io.BufferedWriter;
import java.io.IOException;

import gps.examples.extendedsssp.ExtendedSSSPComputationStage.ComputationStage;
import gps.globalobjects.GlobalObject;
import gps.globalobjects.IntOverwriteGlobalObject;
import gps.globalobjects.IntSumGlobalObject;
import gps.graph.Master;
import gps.writable.IntWritable;
import gps.writable.MinaWritable;

/**
 * Master class for the extended sssp algorithm. It coordinates the flow of
 * the two stages of the algorithm as well as computing the average distance
 * at the very end of the computation.
 *
 * @author semihsalihoglu
 */
public class ExtendedSSSPMaster extends Master {

	private double averageDistance;

	@Override
	public void compute(int superstepNo) {
		GlobalObject<? extends MinaWritable> stageGlobalObject =
			getGlobalObjectsMap().getGlobalObject("comp-stage");
		if (stageGlobalObject == null) {
			getGlobalObjectsMap().putGlobalObject("comp-stage",
				new IntOverwriteGlobalObject(ComputationStage.SSSP_FIRST_SUPERSTEP.getId()));
			getGlobalObjectsMap().putGlobalObject("not-converged-vertices",
				new IntSumGlobalObject(0));
		} else {
			int numActiveVertices = ((IntWritable) getGlobalObjectsMap().getGlobalObject(
				"not-converged-vertices").getValue()).getValue();
			ComputationStage computationStage = ComputationStage.getComputationStageFromId(
				((IntWritable) stageGlobalObject.getValue()).getValue());
			// We clear the non-default global objects because global objects are sticky
			globalObjectsMap.clearNonDefaultObjects();
			System.out.println("Ended stage: " + computationStage);
			switch (computationStage) {
			case SSSP_FIRST_SUPERSTEP:
				getGlobalObjectsMap().putGlobalObject("comp-stage",
					new IntOverwriteGlobalObject(ComputationStage.SSSP_LATER_SUPERSTEP.getId()));
				getGlobalObjectsMap().putGlobalObject("not-converged-vertices",
					new IntSumGlobalObject(0));
				break;
			case SSSP_LATER_SUPERSTEP:
				if (numActiveVertices == 0) {
					// Move to next stage, during which we will compute the averege distance
					// of connected vertices to the source.
					getGlobalObjectsMap().putGlobalObject("comp-stage",
						new IntOverwriteGlobalObject(ComputationStage.AVERAGE_COMPUTATION_STAGE.getId()));
					getGlobalObjectsMap().putOrUpdateGlobalObject("sum-distances",
						new IntSumGlobalObject(0));
					getGlobalObjectsMap().putOrUpdateGlobalObject("num-connected",
						new IntSumGlobalObject(0));
				}
				// Remain the sssp_later_superstep stage
				getGlobalObjectsMap().putGlobalObject("comp-stage",
					new IntOverwriteGlobalObject(ComputationStage.SSSP_LATER_SUPERSTEP.getId()));
				getGlobalObjectsMap().putGlobalObject("not-converged-vertices",
					new IntSumGlobalObject(0));
				break;
			case AVERAGE_COMPUTATION_STAGE:
				int sumOfDistances = ((IntWritable) getGlobalObjectsMap().getGlobalObject(
					"sum-distances").getValue()).getValue();
				int numConnected = ((IntWritable) getGlobalObjectsMap().getGlobalObject(
					"num-connected").getValue()).getValue();
				this.averageDistance = (double) sumOfDistances / numConnected;
				// Terminate computation
				this.continueComputation = false;
				break;
			default:
				System.err.println("Unexpected computation stage for master.");
			}
		}		
	}

	@Override
	public void writeOutput(BufferedWriter bw) throws IOException {
		bw.write("average distance:" + averageDistance + "\n");
		super.writeOutput(bw);
	}
}
