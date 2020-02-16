import java.util.LinkedList;
import java.util.List;

public class MapThread implements Runnable {

	private String filename, fileContents;
	private MapCallback mapCallback;
	private int threadNumber;
	
	public MapThread(final String filename, final String fileContents, final MapCallback mapCallback, int threadNumber){
        this.filename = filename;
        this.fileContents = fileContents;
        this.mapCallback = mapCallback;
        this.threadNumber = threadNumber;
    }
	
	
	@Override
	public void run() {
		List<MappedItem> mappedItems = new LinkedList<MappedItem>();
		String[] words = fileContents.split("\\s+");

		for (String word : words) {
			mappedItems.add(new MappedItem(word, filename));
		}

		mapCallback.mapDone(filename, mappedItems);
		System.out.println("Finished from Thread Number: " + threadNumber);
	}
	
	
	/*public Thread MapThread(final String filename, final String fileContents, final MapCallback mapCallback) {

		Thread t = new Thread(new Runnable() {

			@Override
			public void run() {

				List<MappedItem> mappedItems = new LinkedList<MappedItem>();
				String[] words = fileContents.split("\\s+");

				for (String word : words) {
					mappedItems.add(new MappedItem(word, filename));
				}

				mapCallback.mapDone(filename, mappedItems);
			}
		});

		t.start();

		return t;
	}*/

	
}
