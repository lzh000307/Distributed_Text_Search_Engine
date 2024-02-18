package uk.ac.gla.dcs.bigdata.studentfunctions;
import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.studentstructures.PreDPHCurrentData;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;


public class GetDPH implements MapFunction<PreDPHCurrentData,Double>{

	/**
	 * 
	 */
	int totalTermFrequencyInCorpus;
	double averageDocumentLengthInCorpus;
	long totalDocsInCorpus;
	
	private static final long serialVersionUID = 1892720528892912677L;
	public GetDPH(int totalTermFrequencyInCorpus,double averageDocumentLengthInCorpus,long totalDocsInCorpus) {
		this.totalTermFrequencyInCorpus= totalTermFrequencyInCorpus;
		this.averageDocumentLengthInCorpus= averageDocumentLengthInCorpus;
		this.totalDocsInCorpus = totalDocsInCorpus;
	}
	@Override
	public Double call(PreDPHCurrentData value) throws Exception {
		short termFrequencyInCurrentDocument = (short)value.getTermFrequencyInCurrentDocument();
		RankedResult rr = new RankedResult();

		int currentDocumentLength = value.getCurrentDocumentLength();
		if(totalDocsInCorpus==0 || totalTermFrequencyInCorpus==0) {
			return (double) -100;
		}
		double score = DPHScorer.getDPHScore(termFrequencyInCurrentDocument, totalTermFrequencyInCorpus, currentDocumentLength, averageDocumentLengthInCorpus, totalDocsInCorpus);
		return score;
	}

}
