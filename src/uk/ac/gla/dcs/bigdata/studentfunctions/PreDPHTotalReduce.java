package uk.ac.gla.dcs.bigdata.studentfunctions;
import org.apache.spark.api.java.function.ReduceFunction;

import uk.ac.gla.dcs.bigdata.studentstructures.PreDPHCurrentData;

public class PreDPHTotalReduce implements ReduceFunction<PreDPHCurrentData>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 477560311349615932L;

	@Override
	public PreDPHCurrentData call(PreDPHCurrentData v1, PreDPHCurrentData v2) throws Exception {
		// TODO Auto-generated method stub
		PreDPHCurrentData PreDPHCurrentData = new PreDPHCurrentData();
		PreDPHCurrentData.setCurrentDocumentLength(v1.getCurrentDocumentLength()+v2.getCurrentDocumentLength());
		PreDPHCurrentData.setTermFrequencyInCurrentDocument(v1.getTermFrequencyInCurrentDocument()+v2.getTermFrequencyInCurrentDocument());
		return PreDPHCurrentData;
	}

}
