package debugging;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Phase2 {

	/** Change this values to modify the settings of start.sh */
	private static boolean localTestServer = true;

	/** The number of servers to create */
	private static int serverCount = 2;
	
	/** 
	 * The arguments that would be used to launch start.sh. It could be done with the args of the main method, 
	 * but it is easier to update something here rather than in run configurations.
	 */
	private static String[] arguments = new String[] { serverCount + "" , "-phase" , "2"};
	
	
	/**
	 * Executes the same methods than the script start.sh
	 * @param args
	 */
	public static void main (String[] args) {
		ExecutorService threadPool = Executors.newCachedThreadPool();
		
		if ( localTestServer ) {
			threadPool.execute(new Runnable() {
				@Override
				public void run() {
					recipesService.test.server.TestServer.main(arguments);
				}
			});
			// Delay
			try {
				Thread.sleep(1000);	
			} catch ( Exception e){
				
			}
			
			recipesService.test.server.SendArgsToTestServer.main(arguments);

			// Delay
			try {
				Thread.sleep(3000);	
			} catch ( Exception e){

			}

		}

		for ( int i = 0 ; i < serverCount ; i++ ) {
			threadPool.execute(new Runnable() {

				@Override
				public void run() {
					recipesService.Server.main(arguments);
				}
			});
		}
	}
	
}
