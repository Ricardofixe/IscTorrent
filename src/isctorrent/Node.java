package isctorrent;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A peer in the IscTorrent network.
 *
 * Every node is both a server (it accepts connections and answers searches and
 * block requests for the files in its shared folder) and a client (it connects
 * to other nodes, searches them and downloads files from them in blocks).
 */
public class Node {

	/** Size of each block a file is split into for download. */
	public static final int BLOCK_SIZE = 10_240;

	/** Maximum number of block requests this node serves at the same time. */
	private static final int UPLOAD_THREADS = 5;

	private final ExecutorService uploadPool = Executors.newFixedThreadPool(UPLOAD_THREADS);

	private final String nodeName;
	private final InetAddress address;
	private final int port;
	private final String sharedFolderPath;

	/** Files this node shares, mapped to their SHA-256 hash. Read by connection threads, written by downloads. */
	private final Map<File, String> sharedFiles = new ConcurrentHashMap<>();

	/** Outgoing connections to other nodes, keyed by their port. */
	private final Map<Integer, PeerConnection> peers = new ConcurrentHashMap<>();

	/** Downloads in progress, keyed by file hash. */
	private final Map<String, DownloadTasksManager> downloads = new ConcurrentHashMap<>();

	/** Number of blocks received from each peer during the current download. */
	private final Map<PeerConnection, Integer> blocksPerPeer = new HashMap<>();

	public Node(String nodeName, InetAddress address, int port, String sharedRoot, String folder) {
		this.nodeName = nodeName;
		this.address = address;
		this.port = port;
		this.sharedFolderPath = System.getProperty("user.dir") + File.separator + sharedRoot + File.separator + folder;
		new Server().start();
	}

	public String getName() {
		return nodeName;
	}

	public int getPort() {
		return port;
	}

	// ------------------------------------------------------------------ shared folder

	/** Reads the shared folder and hashes every file in it, so peers can find and download them. */
	public void loadSharedFolder() throws IOException {
		File folder = new File(sharedFolderPath);
		if (!folder.isDirectory() && !folder.mkdirs()) {
			throw new IOException("Could not create shared folder " + folder.getAbsolutePath());
		}
		File[] files = folder.listFiles(file -> file.isFile() && !file.isHidden());
		if (files == null) {
			return;
		}
		for (File file : files) {
			sharedFiles.put(file, sha256(file));
		}
		System.out.println("[" + nodeName + "] Sharing " + sharedFiles.size() + " file(s) from " + folder.getAbsolutePath());
	}

	public static String sha256(File file) throws IOException {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			try (FileInputStream in = new FileInputStream(file)) {
				byte[] buffer = new byte[8192];
				int read;
				while ((read = in.read(buffer)) != -1) {
					digest.update(buffer, 0, read);
				}
			}
			StringBuilder hex = new StringBuilder();
			for (byte b : digest.digest()) {
				hex.append(String.format("%02x", b));
			}
			return hex.toString();
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException("SHA-256 is not available", e);
		}
	}

	// ------------------------------------------------------------------ client side

	/**
	 * Connects to another node. With {@code connectBack}, the other node is asked to
	 * open a connection back, so both sides can search and download from each other.
	 */
	public void connectToPeer(String host, int peerPort, boolean connectBack) {
		if (peerPort == port || peers.containsKey(peerPort)) {
			return;
		}
		PeerConnection peer = new PeerConnection(host, peerPort, connectBack);
		peers.put(peerPort, peer);
		peer.start();
	}

	/** Asks every connected peer for files whose name contains the keyword. */
	public List<FileSearchResult> searchFileByKeyword(String keyword) {
		WordSearchMessage search = new WordSearchMessage(keyword);
		List<FileSearchResult> results = new ArrayList<>();
		for (PeerConnection peer : peers.values()) {
			results.addAll(peer.search(search));
		}
		return results;
	}

	/** Returns the connected peers that hold a file with this exact hash. */
	private List<PeerConnection> peersWithFile(FileSearchResult file) {
		WordSearchMessage search = new WordSearchMessage(file.getName());
		List<PeerConnection> holders = new ArrayList<>();
		for (PeerConnection peer : peers.values()) {
			for (FileSearchResult result : peer.search(search)) {
				if (result.getHash().equals(file.getHash())) {
					holders.add(peer);
					break;
				}
			}
		}
		return holders;
	}

	/**
	 * Downloads the given files at the same time and blocks until all of them are saved.
	 *
	 * @param suppliers filled with "address:port" of each peer and the number of blocks it sent
	 * @return the elapsed time in seconds
	 */
	public int download(List<FileSearchResult> files, Map<String, Integer> suppliers) throws InterruptedException {
		long start = System.currentTimeMillis();
		synchronized (blocksPerPeer) {
			blocksPerPeer.clear();
		}

		List<DownloadTasksManager> managers = new ArrayList<>();
		for (FileSearchResult file : files) {
			DownloadTasksManager manager = new DownloadTasksManager(file, peersWithFile(file));
			downloads.put(file.getHash(), manager);
			managers.add(manager);
			manager.start();
		}
		for (DownloadTasksManager manager : managers) {
			manager.join();
			downloads.remove(manager.file.getHash());
		}

		synchronized (blocksPerPeer) {
			for (Map.Entry<PeerConnection, Integer> entry : blocksPerPeer.entrySet()) {
				PeerConnection peer = entry.getKey();
				suppliers.put(peer.host + ":" + peer.peerPort, entry.getValue());
			}
		}
		return (int) ((System.currentTimeMillis() - start) / 1000);
	}

	/** An outgoing connection to one peer. Requests on it are sent one at a time. */
	public class PeerConnection extends Thread {
		private final String host;
		private final int peerPort;
		private final boolean connectBack;

		/** One lock per connection: a request and its answer must not interleave with another request. */
		private final Lock lock = new ReentrantLock();
		private final CountDownLatch connected = new CountDownLatch(1);

		private Socket socket;
		private ObjectInputStream in;
		private ObjectOutputStream out;

		PeerConnection(String host, int peerPort, boolean connectBack) {
			this.host = host;
			this.peerPort = peerPort;
			this.connectBack = connectBack;
		}

		@Override
		public void run() {
			try {
				socket = new Socket(InetAddress.getByName(host), peerPort);
				// Requests and answers are small and strictly alternate, so send them immediately
				// instead of letting Nagle's algorithm hold them back waiting for an ACK.
				socket.setTcpNoDelay(true);
				out = new ObjectOutputStream(socket.getOutputStream());
				out.flush();
				in = new ObjectInputStream(socket.getInputStream());
				if (connectBack) {
					out.writeObject(new ConnectBackMessage(port));
					out.flush();
				}
				System.out.println("[" + nodeName + "] Connected to " + host + ":" + peerPort);
			} catch (IOException e) {
				System.err.println("[" + nodeName + "] Could not connect to " + host + ":" + peerPort + ": " + e.getMessage());
				peers.remove(peerPort);
			} finally {
				connected.countDown();
			}
		}

		private boolean awaitConnected() {
			try {
				connected.await();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return false;
			}
			return out != null;
		}

		/** Sends a search and returns this peer's results (empty if the peer is unreachable). */
		@SuppressWarnings("unchecked")
		List<FileSearchResult> search(WordSearchMessage search) {
			if (!awaitConnected()) {
				return List.of();
			}
			lock.lock();
			try {
				out.writeObject(search);
				out.flush();
				Object answer = in.readObject();
				return answer instanceof List ? (List<FileSearchResult>) answer : List.of();
			} catch (IOException | ClassNotFoundException e) {
				System.err.println("[" + nodeName + "] Search failed on " + host + ":" + peerPort + ": " + e.getMessage());
				return List.of();
			} finally {
				lock.unlock();
			}
		}

		/** Requests one block and waits for the answer. */
		FileBlockAnswerMessage requestBlock(FileBlockRequestMessage request) throws IOException {
			if (!awaitConnected()) {
				throw new IOException("Not connected to " + host + ":" + peerPort);
			}
			lock.lock();
			try {
				out.writeObject(request);
				out.flush();
				Object answer = in.readObject();
				if (!(answer instanceof FileBlockAnswerMessage)) {
					throw new IOException("Unexpected answer: " + answer);
				}
				return (FileBlockAnswerMessage) answer;
			} catch (ClassNotFoundException e) {
				throw new IOException(e);
			} finally {
				lock.unlock();
			}
		}
	}

	/**
	 * Downloads one file. The file is split into blocks; one thread per peer that
	 * holds the file takes the next missing block, requests it from its peer and
	 * hands the answer back, so faster peers end up sending more blocks. A
	 * CountDownLatch releases this thread once every block has arrived.
	 */
	public class DownloadTasksManager extends Thread {
		private final FileSearchResult file;
		private final List<PeerConnection> holders;
		private final List<FileBlockRequestMessage> pendingBlocks = new ArrayList<>();
		private final List<FileBlockAnswerMessage> receivedBlocks = new ArrayList<>();
		private final CountDownLatch allBlocksReceived;

		DownloadTasksManager(FileSearchResult file, List<PeerConnection> holders) {
			this.file = file;
			this.holders = holders;
			for (long offset = 0; offset < file.getSize(); offset += BLOCK_SIZE) {
				int length = (int) Math.min(BLOCK_SIZE, file.getSize() - offset);
				pendingBlocks.add(new FileBlockRequestMessage(file.getHash(), offset, length));
			}
			allBlocksReceived = new CountDownLatch(pendingBlocks.size());
		}

		synchronized FileBlockRequestMessage nextBlock() {
			return pendingBlocks.isEmpty() ? null : pendingBlocks.remove(0);
		}

		synchronized void returnBlock(FileBlockRequestMessage block) {
			pendingBlocks.add(block);
		}

		synchronized void submitBlock(FileBlockAnswerMessage block) {
			receivedBlocks.add(block);
			allBlocksReceived.countDown();
		}

		@Override
		public void run() {
			if (holders.isEmpty()) {
				System.err.println("[" + nodeName + "] No connected peer has " + file.getName());
				return;
			}
			System.out.println("[" + nodeName + "] Downloading " + file.getName() + " (" + pendingBlocks.size()
					+ " blocks) from " + holders.size() + " peer(s)");
			List<Thread> workers = new ArrayList<>();
			for (PeerConnection peer : holders) {
				Thread worker = new Thread(() -> downloadFrom(peer), "download-" + file.getName() + "-" + peer.peerPort);
				workers.add(worker);
				worker.start();
			}
			try {
				allBlocksReceived.await();
				for (Thread worker : workers) {
					worker.join();
				}
				saveFile();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			} catch (IOException e) {
				System.err.println("[" + nodeName + "] Could not save " + file.getName() + ": " + e.getMessage());
			}
		}

		/** Worker loop for one peer: keep taking blocks until none are left. */
		private void downloadFrom(PeerConnection peer) {
			FileBlockRequestMessage block;
			while ((block = nextBlock()) != null) {
				try {
					submitBlock(peer.requestBlock(block));
					synchronized (blocksPerPeer) {
						blocksPerPeer.merge(peer, 1, Integer::sum);
					}
				} catch (IOException e) {
					// Give the block back so another peer can fetch it, and stop using this peer.
					returnBlock(block);
					System.err.println("[" + nodeName + "] Peer " + peer.peerPort + " failed: " + e.getMessage());
					return;
				}
			}
		}

		/** Writes the blocks in order, then checks the SHA-256 against the hash the peers advertised. */
		private void saveFile() throws IOException {
			receivedBlocks.sort(Comparator.comparingLong(b -> b.offset));
			File output = new File(sharedFolderPath, file.getName());
			try (FileOutputStream out = new FileOutputStream(output)) {
				for (FileBlockAnswerMessage block : receivedBlocks) {
					out.write(block.data);
				}
			}
			String hash = sha256(output);
			if (!hash.equals(file.getHash())) {
				System.err.println("[" + nodeName + "] Hash mismatch for " + file.getName() + ", discarding it");
				output.delete();
				return;
			}
			sharedFiles.put(output, hash);
			System.out.println("[" + nodeName + "] Saved and verified " + output.getAbsolutePath());
		}
	}

	// ------------------------------------------------------------------ server side

	/** Accepts incoming connections and gives each one its own handler thread. */
	private class Server extends Thread {
		@Override
		public void run() {
			try (ServerSocket serverSocket = new ServerSocket(port)) {
				System.out.println("[" + nodeName + "] Listening on port " + port);
				while (true) {
					Socket socket = serverSocket.accept();
					socket.setTcpNoDelay(true);
					new ConnectionHandler(socket).start();
				}
			} catch (IOException e) {
				System.err.println("[" + nodeName + "] Server stopped: " + e.getMessage());
			}
		}
	}

	/** Answers searches and block requests arriving on one incoming connection. */
	private class ConnectionHandler extends Thread {
		private final Socket socket;
		private ObjectOutputStream out;

		ConnectionHandler(Socket socket) {
			this.socket = socket;
		}

		@Override
		public void run() {
			try (socket) {
				out = new ObjectOutputStream(socket.getOutputStream());
				out.flush();
				ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
				while (true) {
					Object message = in.readObject();
					if (message instanceof WordSearchMessage) {
						send(search((WordSearchMessage) message));
					} else if (message instanceof FileBlockRequestMessage) {
						FileBlockRequestMessage request = (FileBlockRequestMessage) message;
						// Block reads run on the shared pool, so at most UPLOAD_THREADS are served at once.
						uploadPool.submit(() -> {
							try {
								send(new FileBlockAnswerMessage(request.hash, request.offset, readBlock(request)));
							} catch (IOException e) {
								System.err.println("[" + nodeName + "] Could not send block: " + e.getMessage());
							}
						});
					} else if (message instanceof ConnectBackMessage) {
						String peerHost = socket.getInetAddress().getHostAddress();
						connectToPeer(peerHost, ((ConnectBackMessage) message).port, false);
					}
				}
			} catch (IOException | ClassNotFoundException e) {
				// The peer closed the connection.
			}
		}

		private synchronized void send(Object message) throws IOException {
			out.writeObject(message);
			out.flush();
			out.reset();
		}

		private List<FileSearchResult> search(WordSearchMessage search) {
			List<FileSearchResult> results = new ArrayList<>();
			for (Map.Entry<File, String> entry : sharedFiles.entrySet()) {
				File file = entry.getKey();
				if (file.getName().contains(search.keyword)) {
					results.add(new FileSearchResult(file.getName(), entry.getValue(), file.length(), address, port));
				}
			}
			return results;
		}

		private byte[] readBlock(FileBlockRequestMessage request) throws IOException {
			File file = sharedFiles.entrySet().stream()
					.filter(entry -> entry.getValue().equals(request.hash))
					.map(Map.Entry::getKey)
					.findFirst()
					.orElseThrow(() -> new IOException("No shared file with hash " + request.hash));
			try (FileInputStream in = new FileInputStream(file)) {
				in.getChannel().position(request.offset);
				return in.readNBytes(request.length);
			}
		}
	}

	// ------------------------------------------------------------------ messages

	public static class WordSearchMessage implements Serializable {
		private static final long serialVersionUID = 1L;
		final String keyword;

		WordSearchMessage(String keyword) {
			this.keyword = keyword;
		}
	}

	public static class FileSearchResult implements Serializable {
		private static final long serialVersionUID = 1L;
		private final String name;
		private final String hash;
		private final long size;
		private final InetAddress address;
		private final int port;

		FileSearchResult(String name, String hash, long size, InetAddress address, int port) {
			this.name = name;
			this.hash = hash;
			this.size = size;
			this.address = address;
			this.port = port;
		}

		public String getName() {
			return name;
		}

		public String getHash() {
			return hash;
		}

		public long getSize() {
			return size;
		}

		public InetAddress getAddress() {
			return address;
		}

		public int getPort() {
			return port;
		}
	}

	public static class FileBlockRequestMessage implements Serializable {
		private static final long serialVersionUID = 1L;
		final String hash;
		final long offset;
		final int length;

		FileBlockRequestMessage(String hash, long offset, int length) {
			this.hash = hash;
			this.offset = offset;
			this.length = length;
		}
	}

	public static class FileBlockAnswerMessage implements Serializable {
		private static final long serialVersionUID = 1L;
		final String hash;
		final long offset;
		final byte[] data;

		FileBlockAnswerMessage(String hash, long offset, byte[] data) {
			this.hash = hash;
			this.offset = offset;
			this.data = data;
		}
	}

	/** Sent right after connecting, asking the other node to open a connection back. */
	public static class ConnectBackMessage implements Serializable {
		private static final long serialVersionUID = 1L;
		final int port;

		ConnectBackMessage(int port) {
			this.port = port;
		}
	}
}
