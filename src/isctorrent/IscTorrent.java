package isctorrent;

import java.io.IOException;
import java.net.InetAddress;

import javax.swing.SwingUtilities;
import javax.swing.UIManager;

/**
 * Starts one IscTorrent node.
 *
 * <pre>
 * java -cp out isctorrent.IscTorrent &lt;port&gt; &lt;folder&gt;
 * </pre>
 *
 * The node listens on {@code port} and shares the files in {@code shared/<folder>}.
 */
public class IscTorrent {

	private static final String SHARED_ROOT = "shared";

	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.err.println("Usage: java -cp out isctorrent.IscTorrent <port> <folder>");
			System.err.println("Example: java -cp out isctorrent.IscTorrent 8081 dl1");
			System.exit(1);
		}
		int port = Integer.parseInt(args[0]);
		String folder = args[1];

		Node node = new Node("Node " + port, InetAddress.getLocalHost(), port, SHARED_ROOT, folder);
		node.loadSharedFolder();
		SwingUtilities.invokeLater(() -> {
			try {
				UIManager.setLookAndFeel("javax.swing.plaf.nimbus.NimbusLookAndFeel");
			} catch (Exception e) {
				// Keep the default look and feel.
			}
			new GUI(node).setVisible(true);
		});
	}
}
