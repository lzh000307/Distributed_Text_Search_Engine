package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;

public class PreDPHCurrentData implements Serializable{
	int termFrequencyInCurrentDocument;
	int currentDocumentLength;

	
	public PreDPHCurrentData() {}
	
	
    public int getTermFrequencyInCurrentDocument() {
        return termFrequencyInCurrentDocument;
    }


    public int getCurrentDocumentLength() {
        return currentDocumentLength;
    }
    
    public void setTermFrequencyInCurrentDocument(int termFrequencyInCurrentDocument) {
        this.termFrequencyInCurrentDocument = termFrequencyInCurrentDocument;
    }

    public void setCurrentDocumentLength(int currentDocumentLength) {
        this.currentDocumentLength = currentDocumentLength;
    }
	

}
