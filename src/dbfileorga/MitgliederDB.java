package dbfileorga;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

public class MitgliederDB implements Iterable<Record>
{
	
	protected DBBlock db[] = new DBBlock[8];
	
	
	public MitgliederDB(boolean ordered){
		this();
		insertMitgliederIntoDB(ordered);
		
	}
	public MitgliederDB(){
		initDB();
	}
	
	private void initDB() {
		for (int i = 0; i<db.length; ++i){
			db[i]= new DBBlock();
		}
		
	}
	private void insertMitgliederIntoDB(boolean ordered) {
		MitgliederTableAsArray mitglieder = new MitgliederTableAsArray();
		String mitgliederDatasets[];
		if (ordered){
			mitgliederDatasets = mitglieder.recordsOrdered;
		}else{
			mitgliederDatasets = mitglieder.records;
		}
		for (String currRecord : mitgliederDatasets ){
			appendRecord(new Record(currRecord));
		}	
	}

		
	protected int appendRecord(Record record){
		//search for block where the record should be appended
		int currBlock = getBlockNumOfRecord(getNumberOfRecords());
		int result = db[currBlock].insertRecordAtTheEnd(record);
		if (result != -1 ){ //insert was successful
			return result;
		}else if (currBlock < db.length) { // overflow => insert the record into the next block
			return db[currBlock+1].insertRecordAtTheEnd(record);
		}
		return -1;
	}
	

	@Override
	public String toString(){
		String result = new String();
		for (int i = 0; i< db.length ; ++i){
			result += "Block "+i+"\n";
			result += db[i].toString();
			result += "-------------------------------------------------------------------------------------\n";
		}
		return result;
	}


	public int getPosInBlock(int numRec){

		int counter = 0;

		for (int i = 0; i< db.length; ++i){
			DBBlock d = getBlock(i);
			for(int k = 1; k<= d.getNumberOfRecords(); k++){
				counter++;
				if(counter == numRec){
					return  k;

				}
			}
		}
		return -1;
	}




	/**
	 * Returns the number of Records in the Database
	 * @return number of records stored in the database
	 */
	public int getNumberOfRecords(){
		int result = 0;
		for (DBBlock currBlock: db){
			result += currBlock.getNumberOfRecords();
		}
		return result;
	}
	
	/**
	 * Returns the block number of the given record number 
	 * @param recNum the record number to search for
	 * @return the block number or -1 if record is not found
	 */
	public int getBlockNumOfRecord(int recNum){
		int recCounter = 0;
		for (int i = 0; i< db.length; ++i){
			if (recNum <= (recCounter+db[i].getNumberOfRecords())){
				return i ;
			}else{
				recCounter += db[i].getNumberOfRecords();
			}
		}
		return -1;
	}
		
	public DBBlock getBlock(int i){
		return db[i];
	}
	
	
	/**
	 * Returns the record matching the record number
	 * @param recNum the term to search for
	 * @return the record matching the search term
	 */
	public Record read(int recNum){
		Iterator<Record> it = iterator();
		Record rec = null;

		for (int i = 1; i<=recNum; i++){
			rec = it.next();
		}

		return rec;
	}
	
	/**
	 * Returns the number of the first record that matches the search term
	 * @param searchTerm the term to search for
	 * @return the number of the record in the DB -1 if not found
	 */
	public int findPos(String searchTerm){
		Iterator<Record> it = iterator();
		int erg = -1;
		int counter = 1;

		while(it.hasNext()){

			if(it.next().getAttribute(1).equals(searchTerm)){
				erg = counter;
				break;
			}
			counter += 1;
		}

		return erg;
	}
	
	/**
	 * Inserts the record into the file and returns the record number
	 * @param record
	 * @return the record number of the inserted record
	 */
	public int insert(Record record){ // Soll hier in die File eingefügt werden oder nur appended ?
		this.appendRecord(record);
		return getNumberOfRecords();
	}
	
	/**
	 * Deletes the record specified 
	 * @param numRecord number of the record to be deleted
	 */
	public void delete(int numRecord){
		//TODO implement
		int currBlock = getBlockNumOfRecord(numRecord);
		int RecordPos =getPosInBlock(numRecord);
		int prevRecEndPos ;

		DBBlock block = getBlock(currBlock);
		Record nextRecord= new Record("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab"); //TODO: Ein weg finden den richtigen Record zu bekommen



		// Gets the Previous Record to find EndPos. EndPos = StartPos of New Record
		if (RecordPos == 1){
			prevRecEndPos = 0;

		}
		else{
			prevRecEndPos = block.getRecord(getPosInBlock(numRecord-1)).length()+1;
		}

		for(int i = numRecord;i <=getNumberOfRecords(); i++){
			int o = block.moveRecordToPos(prevRecEndPos, nextRecord);
			//TODO: Ende eines Block mit /u000 füllen s;
			if(o == -1){
				break;
			}

			prevRecEndPos = o + 1;

		}



	}
	
	/**
	 * Replaces the record at the specified position with the given one.
	 * @param numRecord the position of the old record in the db
	 * @param record the new record
	 * 
	 */
	public void modify(int numRecord, Record record){
		//TODO implement
		DBBlock block = getBlock(getBlockNumOfRecord(numRecord));
		int RecordPos =getPosInBlock(numRecord);
		Record rec = null;

		// Gets the Previous Record to find EndPos. EndPos = StartPos of New Record
		if (RecordPos == 1){
			 rec = block.getRecord(getPosInBlock(numRecord));
		}
		else{
			rec = block.getRecord(getPosInBlock(numRecord-1));
		}



		int length = rec.length();



	}

	
	@Override
	public Iterator<Record> iterator() {
		return new DBIterator();
	}
 
	private class DBIterator implements Iterator<Record> {

		    private int currBlock = 0;
		    private Iterator<Record> currBlockIter= db[currBlock].iterator();

	        public boolean hasNext() {
	            if (currBlockIter.hasNext()){
	                return true;
	            }else if (currBlock < db.length){ //continue search in the next block
	            	return db[currBlock+1].iterator().hasNext();
	            }else{ 
	                return false;
	            }
	        }

	        public Record next() {	        	
	        	if (currBlockIter.hasNext()){
	        		return currBlockIter.next();
	        	}else if (currBlock < db.length){ //continue search in the next block
	        		currBlockIter= db[++currBlock].iterator();
	        		return currBlockIter.next();
	        	}else{
	        		throw new NoSuchElementException();
	        	}
	        }
	 
	        @Override
	        public void remove() {
	        	throw new UnsupportedOperationException();
	        }
	    } 
	 

}
