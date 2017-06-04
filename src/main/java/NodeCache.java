import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by lleywyn on 17-5-1.
 */
class NodeCache<T> {
    private List<T> nodes = new ArrayList<T>();
    private List<T> addedNodes = new ArrayList<T>();
    private List<T> deletedNodes = new ArrayList<T>();

    public NodeCache() {
    }

    public void refresh(List<T> list) {
        this.nodes = list;
        this.addedNodes = (List<T>) CollectionUtils.removeAll(list, nodes);
        this.deletedNodes = (List<T>) CollectionUtils.removeAll(nodes, list);
    }

    public boolean isEmpty() {
        return CollectionUtils.isEmpty(nodes);
    }

    public List<T> getNodes() {
        return nodes;
    }

    public List<T> getAddedNodes() {
        return addedNodes;
    }

    public List<T> getDeletedNodes() {
        return deletedNodes;
    }
}
