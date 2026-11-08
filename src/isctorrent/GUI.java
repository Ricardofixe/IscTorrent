package isctorrent;

import java.awt.BorderLayout;
import java.awt.GridLayout;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.swing.BorderFactory;
import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;
import javax.swing.SwingUtilities;

import isctorrent.Node.FileSearchResult;

/** Swing window for one node: search connected peers, download files and connect to new peers. */
@SuppressWarnings("serial")
public final class GUI extends JFrame {

	private static final long serialVersionUID = 1L;

	private final Node node;
	private final JTextField searchField = new JTextField(20);
	private final JButton searchButton = new JButton("Search");
	private final JButton downloadButton = new JButton("Download");
	private final JButton connectButton = new JButton("Connect to node");
	private final DefaultListModel<String> resultsModel = new DefaultListModel<>();
	private final JList<String> resultsList = new JList<>(resultsModel);

	/** One entry per distinct file shown in the list, mapped to one of the results for it. */
	private final Map<String, FileSearchResult> resultByEntry = new HashMap<>();

	public GUI(Node node) {
		this.node = node;
		setTitle("IscTorrent - " + node.getName() + " (port " + node.getPort() + ")");
		setSize(800, 400);
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setLocationRelativeTo(null);
		layoutComponents();
		searchButton.addActionListener(e -> search());
		searchField.addActionListener(e -> search());
		downloadButton.addActionListener(e -> download());
		connectButton.addActionListener(e -> connect());
	}

	private void layoutComponents() {
		resultsList.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);

		JPanel top = new JPanel(new BorderLayout(8, 0));
		top.setBorder(BorderFactory.createEmptyBorder(8, 8, 8, 8));
		top.add(new JLabel("Search for:"), BorderLayout.WEST);
		top.add(searchField, BorderLayout.CENTER);
		top.add(searchButton, BorderLayout.EAST);

		JPanel right = new JPanel(new GridLayout(2, 1, 0, 8));
		right.setBorder(BorderFactory.createEmptyBorder(0, 0, 8, 8));
		right.add(downloadButton);
		right.add(connectButton);

		JScrollPane results = new JScrollPane(resultsList);
		results.setBorder(BorderFactory.createCompoundBorder(
				BorderFactory.createEmptyBorder(0, 8, 8, 8), results.getBorder()));

		setLayout(new BorderLayout());
		add(top, BorderLayout.NORTH);
		add(results, BorderLayout.CENTER);
		add(right, BorderLayout.EAST);
	}

	private void search() {
		String keyword = searchField.getText().trim();
		if (keyword.isEmpty()) {
			JOptionPane.showMessageDialog(this, "Type part of a file name to search for.", "Search",
					JOptionPane.WARNING_MESSAGE);
			return;
		}
		List<FileSearchResult> results = node.searchFileByKeyword(keyword);
		if (results.isEmpty()) {
			JOptionPane.showMessageDialog(this, "No results for \"" + keyword + "\".", "Search",
					JOptionPane.INFORMATION_MESSAGE);
		}
		showResults(results);
	}

	/** Groups results by file hash and shows each file once, with the number of peers that have it. */
	private void showResults(List<FileSearchResult> results) {
		Map<String, List<FileSearchResult>> byHash = new LinkedHashMap<>();
		for (FileSearchResult result : results) {
			byHash.computeIfAbsent(result.getHash(), h -> new ArrayList<>()).add(result);
		}
		resultsModel.clear();
		resultByEntry.clear();
		for (List<FileSearchResult> copies : byHash.values()) {
			FileSearchResult first = copies.get(0);
			String entry = first.getName() + "  (" + copies.size() + (copies.size() == 1 ? " peer)" : " peers)");
			resultsModel.addElement(entry);
			resultByEntry.put(entry, first);
		}
	}

	private void download() {
		List<FileSearchResult> selected = new ArrayList<>();
		for (String entry : resultsList.getSelectedValuesList()) {
			selected.add(resultByEntry.get(entry));
		}
		if (selected.isEmpty()) {
			JOptionPane.showMessageDialog(this, "Select one or more files to download.", "Download",
					JOptionPane.WARNING_MESSAGE);
			return;
		}

		// Run the download off the Swing thread so the window stays responsive.
		downloadButton.setEnabled(false);
		Thread worker = new Thread(() -> {
			Map<String, Integer> suppliers = new LinkedHashMap<>();
			String message;
			try {
				int seconds = node.download(selected, suppliers);
				message = downloadSummary(suppliers, seconds);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				message = "Download interrupted.";
			}
			String summary = message;
			SwingUtilities.invokeLater(() -> {
				downloadButton.setEnabled(true);
				JOptionPane.showMessageDialog(this, summary, "Download finished", JOptionPane.INFORMATION_MESSAGE);
			});
		}, "download-ui");
		worker.start();
	}

	private static String downloadSummary(Map<String, Integer> suppliers, int seconds) {
		StringBuilder text = new StringBuilder("Download complete.\n\nBlocks received per peer:\n");
		for (Map.Entry<String, Integer> entry : suppliers.entrySet()) {
			text.append("  ").append(entry.getKey()).append(": ").append(entry.getValue()).append('\n');
		}
		return text.append("\nElapsed time: ").append(seconds).append(" s").toString();
	}

	private void connect() {
		JTextField hostField = new JTextField("localhost", 10);
		JTextField portField = new JTextField("808", 5);
		JPanel form = new JPanel();
		form.add(new JLabel("Address:"));
		form.add(hostField);
		form.add(new JLabel("Port:"));
		form.add(portField);

		int choice = JOptionPane.showConfirmDialog(this, form, "Connect to node", JOptionPane.OK_CANCEL_OPTION,
				JOptionPane.PLAIN_MESSAGE);
		if (choice != JOptionPane.OK_OPTION) {
			return;
		}
		try {
			node.connectToPeer(hostField.getText().trim(), Integer.parseInt(portField.getText().trim()), true);
		} catch (NumberFormatException e) {
			JOptionPane.showMessageDialog(this, "The port must be a number.", "Connect to node",
					JOptionPane.ERROR_MESSAGE);
		}
	}
}
