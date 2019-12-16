package org.kafkablocks.utils;

import lombok.Getter;
import lombok.Setter;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;


public class TreeNode<T> {
    @Getter
    @Setter
    private T data;
    @Getter
    private TreeNode<T> parent;
    @Getter
    private final List<TreeNode<T>> children;

    public TreeNode(T data) {
        this.data = data;
        this.children = new LinkedList<>();
    }

    public TreeNode<T> addChild(T child) {
        return addChild(new TreeNode<>(child));
    }

    public TreeNode<T> addChild(TreeNode<T> childNode) {
        childNode.parent = this;
        this.children.add(childNode);
        return childNode;
    }

    public void traverse(Consumer<T> nodeConsumer) {
        nodeConsumer.accept(this.data);
        for (TreeNode<T> child : children) {
            child.traverse(nodeConsumer);
        }
    }

    public boolean isRoot() {
        return parent == null;
    }

    public boolean isLeaf() {
        return children.size() == 0;
    }

    public int getLevel() {
        if (this.isRoot())
            return 0;
        else
            return parent.getLevel() + 1;
    }

    public boolean isChild(T object) {
        return children.stream().anyMatch(node -> node.getData().equals(object));
    }

    @Override
    public String toString() {
        return data != null ? data.toString() : "[data null]";
    }
}
