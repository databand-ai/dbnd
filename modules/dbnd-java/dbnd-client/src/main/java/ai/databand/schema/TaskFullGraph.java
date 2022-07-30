/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TaskFullGraph {

    private String rootTaskRunUid;
    private int root;
    private Map<String, NodeInfo> nodesInfo;
    private List<NodeRelationInfo> children;
    private List<NodeRelationInfo> upstreams;

    public String getRootTaskRunUid() {
        return rootTaskRunUid;
    }

    public void setRootTaskRunUid(String rootTaskRunUid) {
        this.rootTaskRunUid = rootTaskRunUid;
    }

    public int getRoot() {
        return root;
    }

    public void setRoot(int root) {
        this.root = root;
    }

    public Map<String, NodeInfo> getNodesInfo() {
        return nodesInfo;
    }

    public void setNodesInfo(Map<String, NodeInfo> nodesInfo) {
        this.nodesInfo = nodesInfo;
    }

    public List<NodeRelationInfo> getChildren() {
        return children;
    }

    public void setChildren(List<NodeRelationInfo> children) {
        this.children = children;
    }

    public List<NodeRelationInfo> getUpstreams() {
        return upstreams;
    }

    public void setUpstreams(List<NodeRelationInfo> upstreams) {
        this.upstreams = upstreams;
    }
}
