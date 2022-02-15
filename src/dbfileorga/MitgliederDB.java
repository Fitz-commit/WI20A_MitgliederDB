package dbfileorga;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

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


	public int getStartPos(int RecordPosInBlock, DBBlock block){
		int prevRecEndPos = 0;

		for( int i = 1; i <RecordPosInBlock; i++){
			prevRecEndPos += block.getRecord(i).length()+1;
		}

		return prevRecEndPos;
	}

	public int fillUpBlock(int startPos, int blockNum){
		DBBlock currBlock = getBlock(blockNum);
		if(currBlock.findEmptySpace() ==0){
			currBlock.delete();
			return 0;
		}

		if(blockNum != db.length-1) {
			DBBlock nextBlock = getBlock(blockNum + 1);
			Record nextRec;
			int counter = 0; //Zählt wie viele Sätze vom nächsten block in den zu füllenden Block gesetzt wurden


			int sign;
			for (int i = 1; i <= nextBlock.getNumberOfRecords(); i++) {

				nextRec = nextBlock.getRecord(i);

				sign = currBlock.moveRecordToPos(startPos, nextRec);

				if (sign == -1) {
					cleanout(startPos, blockNum);
					break;
				}

				startPos = sign +1;
				counter++;
			}

			currBlock.addRECDEL(startPos-1);
			return counter;
		}
		return 0;
	}

	private void cleanout(int pos, int currBlockNum) {
		DBBlock block = getBlock(currBlockNum);

		for(int i = pos; i <  DBBlock.BLOCKSIZE; i++){
			block.clearData(i);
		}
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
		int counter = 1;

		while (it.hasNext()){

			if(counter == recNum){
				rec = it.next();
				break;
			}

			it.next();
			counter+= 1;
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
	public int insert(Record record){
		this.appendRecord(record); //TODO: Hier prüfen ob es wirklich am letzten Block angesetzt wird
		return getNumberOfRecords();
	}
	
	/**
	 * Deletes the record specified 
	 * @param numRecord number of the record to be deleted
	 */
	public void delete(int numRecord){
		int currBlockNum = getBlockNumOfRecord(numRecord);
		int RecordPosInBlock = getPosInBlock(numRecord);

		cleaner(RecordPosInBlock, currBlockNum);
	}

	private void cleaner(int RecordPosInBlock, int currBlockNum) {
		DBBlock block = getBlock(currBlockNum);
		int endPos = 0;


		endPos = reorganizeInBlock(RecordPosInBlock, currBlockNum);


		int amountOfRecords= fillUpBlock(endPos, currBlockNum);



		if(amountOfRecords == 0 ){
			cleanout(endPos,currBlockNum);
		}else{
			for(int i = 1; i <= amountOfRecords; i++){
				cleaner(1, currBlockNum+1);
			}
		}
	}


	private int reorganizeInBlock(int RecordPosInBlock, int BlockNum) { //RecordPos = Satz den ich überschreiben möchte

		DBBlock block = getBlock(BlockNum);
		int startPos = getStartPos(RecordPosInBlock, block); //StartPos = Start Pos des Satzes der zu Überschreiben ist


		List<Record> list = new ArrayList<Record>();

		for(int i = RecordPosInBlock+1; i <= block.getNumberOfRecords(); i++){
			list.add(block.getRecord(i));
		}

		for (Record rec: list) {
			int o = block.moveRecordToPos(startPos,rec);
			startPos = o+1;
		}

		cleanout(startPos,BlockNum);
		return startPos;
	}


	/**
	 * Replaces the record at the specified position with the given one.
	 * @param numRecord the position of the old record in the db
	 * @param record the new record
	 * 
	 */
	public void modify(int numRecord, Record record) {

		int blockNum = getBlockNumOfRecord(numRecord);
		DBBlock block = getBlock(blockNum);
		int RecordPosInBlock =getPosInBlock(numRecord);
		int startpos = getStartPos(RecordPosInBlock,block);
		int pos = block.moveRecordToPos(startpos,record);

		List<Record> TransferList = new ArrayList<>();
		List<Record> CopyofBlock = new ArrayList<>();


		fillUpBlock(block.findEmptySpace(), blockNum); // Wenn der Satz so modifiziert das der folgende Satz im nächsten Block noch reinpasst


		for (int i = RecordPosInBlock +1; i<= block.getNumberOfRecords(); i++) {
			CopyofBlock.add(block.getRecord(i));
		}


		//Wenn der letzte Satz modifiziert wird und zu groß wird ODER wenn von vornherein ein zu großer Satz eingestellt wird (>256 Zeichen)
		if(pos == -1){
			if(record.length() > DBBlock.BLOCKSIZE){
				System.out.println("Record is too long!");
				return;
			}

			TransferList.add(record);
			cleanout(getStartPos(RecordPosInBlock,block),blockNum);
			writeRecordInNextBlock(TransferList, blockNum);

			return;



		}



		int a = pos;
		int b = 0;
		for (Record rec: CopyofBlock) {
			if(a !=-1){
				b = a;
				a = block.moveRecordToPos(a+1,rec);
			}
			if(a == -1){
				TransferList.add(rec);

			}
		}

		if(!TransferList.isEmpty()) {
			writeRecordInNextBlock(TransferList, blockNum);
			block.addRECDEL(b);
		}else{
			b=a;
		}

		cleanout(b + 1, blockNum);
	}

	private int writeRecordInNextBlock(List<Record> TransferList, int blockNum) { //Returns pos in next Block
		blockNum++;
		DBBlock block = getBlock(blockNum);
		int sign = 0;
		int StartPos =-1;

		List<Record> Copy = new ArrayList<>();

		for(Record rec: block){
			Copy.add(rec);
		}

		Iterator<Record> CopyIt = Copy.iterator();


		for (Record rec: TransferList) {
			StartPos = block.moveRecordToPos(StartPos+1, rec);

		}

		cleanout(StartPos,blockNum);
		TransferList.clear();
		Record oldRec = null;
		block.addRECDEL(StartPos);
		sign = StartPos;
		while(CopyIt.hasNext()){
			oldRec = CopyIt.next();
			if(sign != -1){
				sign = block.moveRecordToPos(sign+1, oldRec);
			}

			if(sign == -1){
				TransferList.add(oldRec);
			}


		}

		if(!TransferList.isEmpty()){
			writeRecordInNextBlock(TransferList,blockNum);
		}

		return 0;
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
