import java.util.ArrayList;

/**
 * 
 */

/**
 * @author kushal
 *
 */
public class Node {
	/**
	 * 
	 * 
	 */
	private ArrayList<String> list;
	private Double intermediateRank;
	private Double cumulativeRank;
	private String nodeId;
	private String nodeString;

	Node (String nodeId) {
		this.nodeId = nodeId;
	}
	Node(String nodeId, Double cumRank, String adjString) {
		Node node = new Node(nodeId);
		node.setCumulativeRank(cumRank);
		String[] arr2 = adjString.split(",");
		for (int i = 0; i < arr2.length; i++) {
			node.addNeighbour(arr2[i]);
		}
	}
	
	/*Node(String nodeString) {
		this.nodeString = nodeString;
		String[] arr = nodeString.split(" ");
		assert(arr.length == 2);
		// first elment of nodeString is nodeId
		this.nodeId = arr[0];
		
		// second element is pageRank
		this.cumulativeRank = Double.parseDouble(arr[1]);
	}
*/
	public String getNodeString() {
		return nodeString;
	}

	public void setNodeString(String nodeString) {
		this.nodeString = nodeString;
	}

	public Double getIntermediateRank() {
		return intermediateRank;
	}

	public void setIntermediateRank(Double intermediateRank) {
		this.intermediateRank = intermediateRank;
	}

	public Double getCumulativeRank() {
		return cumulativeRank;
	}

	public void setCumulativeRank(Double cumulativeRank) {
		this.cumulativeRank = cumulativeRank;
	}

	public String getNodeId() {
		return nodeId;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	public ArrayList<String> getList() {
		return list;
	}

	public void setList(ArrayList<String> list) {
		this.list = list;
	}

	
	public void addNeighbour(String nId) {
		list.add(nId);
	}

	public String toString() {
		return nodeId + " " + cumulativeRank.toString();
	}
}
