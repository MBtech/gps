package gps.examples.scc.serial;

import java.util.LinkedList;
import java.util.List;

public class Node {

    public final int id;
    public List<Node> children;
    public int dfsSearchIndex;
    public Color dfsSearchColor;
    
    public Node(int id) {
      this.id = id;
      this.children = new LinkedList<Node>();
      this.dfsSearchIndex = 0;
      this.dfsSearchColor = Color.WHITE;
    }
    
    public static enum Color {
      WHITE,
      GRAY,
      BLACK
    }
}
