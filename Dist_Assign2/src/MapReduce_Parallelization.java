import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MapReduce_Parallelization {
	
	private static BlockingQueue<Thread> blockingQueue = new ArrayBlockingQueue<Thread>(3);
	private final static int UPPER_LIMIT = 2;
	
	public static void main(String[] args) throws InterruptedException {
	
		// kevin Cusack
		/*Map<String, Integer> countByWords = new HashMap<String, Integer>();
	    Scanner s = null;
	    
		try {
			s = new Scanner(new File("c:\\Test\\essay.txt"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
	    while (s.hasNext()) {

	        String next = s.next();
	        // This does the same as trim();
	        next = next.replaceAll("\\s+", "");
	        System.out.print(next + " ");
	        
	        Integer count = countByWords.get(next);
	        
	        if (count != null) {
	            countByWords.put(next, count + 1);
	        } else {
	            countByWords.put(next, 1);
	        }
	    }
	    
		if (!countByWords.isEmpty()) {
			System.out.println("\ntotal 'kevin' in the text file Is: " + countByWords.get("kevin").intValue());
			System.out.println("Number of different words Is: " + countByWords.size());
		}
		
	    s.close();*/
	
			
		
		// MAP:		
		final List<MappedItem> mappedItems = new LinkedList<MappedItem>();
		ExecutorService executor = Executors.newFixedThreadPool(UPPER_LIMIT);
		
		MapCallback mapCallback = new MapCallback() {

			@Override
			public synchronized void mapDone(String filename, List<MappedItem> values) {
				mappedItems.addAll(values);
			}
		};
		
		//MapThread[] maps = new MapThread[6];
		//MapThread maps = new MapThread("file4.txt", "foo foo bar cat dog dog", mapCallback);
		executor.execute(new MapThread("file1.txt", "foo foo bar cat dog dog", mapCallback, 1));
		executor.execute(new MapThread("file2.txt", "foo house cat cat dog", mapCallback, 2));
		executor.execute(new MapThread("file3.txt", "foo bird foo foo", mapCallback, 3));
		executor.execute(new MapThread("file4.txt", "mouse bird cow", mapCallback, 4));
		
		try {
			// =============== CHANGE THIS VALUE FOR ASSIGNMENT ===============
            Thread.sleep(40);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }	
		executor.shutdown();
		
		//blockingQueue.add(mapThread("file1.txt", "foo foo bar cat dog dog", mapCallback));
		//blockingQueue.add(mapThread("file2.txt", "foo house cat cat dog", mapCallback));
		//blockingQueue.add(mapThread("file3.txt", "foo bird foo foo", mapCallback));
		
		
		/*ArrayList<Thread> mapThreads = new ArrayList<Thread>();		
		mapThreads.add(mapThread("file1.txt", "foo foo bar cat dog dog", mapCallback));
		mapThreads.add(mapThread("file2.txt", "foo house cat cat dog", mapCallback));
		mapThreads.add(mapThread("file3.txt", "foo bird foo foo", mapCallback));	*/	
		//waitForAllThreadsToFinish(blockingQueue); // blocking call...
		
		System.out.println("MappedItems: " + mappedItems);
		
		
		
		
		// GROUP:		
		Map<String, List<String>> groupedItems = group(mappedItems);
		System.out.println("\nGrouped Items Size: " + groupedItems.size() + ", Items: " + groupedItems);		
		System.out.println("Foo Grouped Items: " + groupedItems.get("foo") + "\n");
		
		final Map<String, Map<String, FileMatch>> index = new HashMap<String, Map<String, FileMatch>>();	
		ReduceCallback reduceCallback = new ReduceCallback() {

			@Override
			public synchronized void reduceDone(String word, Map<String, FileMatch> reducedMap) {
				index.put(word, reducedMap);
			}
		};
		
		//System.out.println("GroupedItem " + groupedItems);
		ArrayList<Thread> reduceThreads = new ArrayList<Thread>();	
		
		Iterator<Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
		
		while(groupedIter.hasNext()) {
			
			Entry<String, List<String>> entry = groupedIter.next();			
			String word = entry.getKey();
			List<String> list = entry.getValue();
			
			// REDUCE:
			reduceThreads.add(reduce(word, list, reduceCallback));
		}
		
		waitsForAllThreadsToFinish(reduceThreads); // blocking call...		
		printIndex(index);
	}
	
	
	public static Thread mapThread(final String filename, final String fileContents, final MapCallback mapCallback) {
		
		Thread t = new Thread(new Runnable() {

			@Override
			public void run() {

				//synchronized (queue) {
					//System.out.println("Queue size is " + queue.size());
					List<MappedItem> mappedItems = new LinkedList<MappedItem>();
					String[] words = fileContents.split("\\s+");

					for (String word : words) {
						mappedItems.add(new MappedItem(word, filename));
					}

					mapCallback.mapDone(filename, mappedItems);
				//}
			}
		});
		
		t.start();
		
		return t;
	}
	
	
	public static Map<String, List<String>> group(List<MappedItem> mappedItems) {
		
		Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();		 
		Iterator<MappedItem> iter = mappedItems.iterator();
		
		while(iter.hasNext()) {
			
		    MappedItem item = iter.next();		    
		    String word = item.getWord();
		    String file = item.getFile();		    
		    List<String> list = groupedItems.get(word);
		    
		    if (list == null) {
		        list = new LinkedList<String>();
		        groupedItems.put(word, list);
		    }
		    
		    list.add(file);
		}
		
		return groupedItems;
	}
	
	public static Thread reduce(final String word, final List<String> list, final ReduceCallback reduceCallback) {
		
		Thread t = new Thread(new Runnable() {

			@Override
			public void run() {
				
				Map<String, FileMatch> reducedMap = new HashMap<String, FileMatch>();
				
				for (String filename : list) {
					
					FileMatch fileMatch = reducedMap.get(filename);
					
					if (fileMatch == null) {
						fileMatch = new FileMatch(filename);
						reducedMap.put(filename, fileMatch);
					}
					
					fileMatch.inc();
				}	
				
				reduceCallback.reduceDone(word, reducedMap);
			}
		});
		
		t.start();
		
		return t;
	}
	
	public static void printIndex(Map<String, Map<String, FileMatch>> index) {
		
		Iterator<Entry<String, Map<String, FileMatch>>> iter1 = index.entrySet().iterator();
		
		while(iter1.hasNext()) {
			
			Entry<String, Map<String, FileMatch>> entry1 = iter1.next();
			
			String word = entry1.getKey();
			Collection<FileMatch> fileMatches = entry1.getValue().values();
			
			System.out.print(word + " => [ ");			
			boolean firstElement = true;
			
			for(FileMatch fileMatch: fileMatches) {
				if (firstElement) {
					firstElement = false;
				} else {
					System.out.print(", ");
				}
				
				System.out.print(fileMatch);
			}
			
			System.out.println(" ]");
		}
	}
	
	public static void waitForAllThreadsToFinish(BlockingQueue<Thread> queue) {
		
		try {
			
			for(Thread t: queue) 
				t.join();
			
		} catch(InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static void waitsForAllThreadsToFinish(ArrayList<Thread> reduceThreads) {

		try {

			for (Thread t : reduceThreads)
				t.join();

		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
}