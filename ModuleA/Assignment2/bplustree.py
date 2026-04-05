from bisect import bisect_left, bisect_right

class BPlusTreeNode:
    # Initialize a tree node as either a leaf or internal node container.
    def __init__(self, leaf=False):
        self.leaf = leaf
        self.keys = []
        self.values = []
        self.children = []
        self.next = None
        self.parent = None

class BPlusTree:
    # Initialize the B+ tree with a minimum degree and an empty leaf root.
    def __init__(self, t=3):
        if t < 2:
            raise ValueError("B+ Tree minimum degree t must be >= 2")
        self.root = BPlusTreeNode(leaf=True)
        self.t = t

    # Traverse down from the root to find the leaf that should contain the key.
    def _find_leaf(self, key):
        node = self.root
        while not node.leaf:
            idx = bisect_right(node.keys, key)
            node = node.children[idx]
        return node

    # Return the leftmost leaf node for full in-order scans.
    def _leftmost_leaf(self):
        node = self.root
        while not node.leaf:
            node = node.children[0]
        return node

    # Look up and return the value associated with a key, or None if absent.
    def search(self, key):
        leaf = self._find_leaf(key)
        idx = bisect_left(leaf.keys, key)
        if idx < len(leaf.keys) and leaf.keys[idx] == key:
            return leaf.values[idx]
        return None

    # Insert or update a key-value pair while preserving B+ tree balance rules.
    def insert(self, key, value):
        root = self.root
        if len(root.keys) == (2 * self.t) - 1:
            temp = BPlusTreeNode()
            self.root = temp
            temp.children.insert(0, root)
            root.parent = temp
            self._split_child(temp, 0)
            self._insert_non_full(temp, key, value)
        else:
            self._insert_non_full(root, key, value)

    # Insert a key-value pair into a node that is guaranteed not to be full.
    def _insert_non_full(self, node, key, value):
        if node.leaf:
            idx = bisect_left(node.keys, key)
            if idx < len(node.keys) and node.keys[idx] == key:
                node.values[idx] = value
                return
            node.keys.insert(idx, key)
            node.values.insert(idx, value)
        else:
            idx = bisect_right(node.keys, key)
            if len(node.children[idx].keys) == (2 * self.t) - 1:
                self._split_child(node, idx)
                if key >= node.keys[idx]:
                    idx += 1
            self._insert_non_full(node.children[idx], key, value)

    # Split a full child node and promote the separator key into its parent.
    def _split_child(self, parent, index):
        t = self.t
        child = parent.children[index]
        new_node = BPlusTreeNode(leaf=child.leaf)
        new_node.parent = parent

        if child.leaf:
            mid = t - 1
            new_node.keys = child.keys[mid:]
            new_node.values = child.values[mid:]
            child.keys = child.keys[:mid]
            child.values = child.values[:mid]
            new_node.next = child.next
            child.next = new_node

            parent.keys.insert(index, new_node.keys[0])
            parent.children.insert(index + 1, new_node)
        else:
            mid = t - 1
            promote_key = child.keys[mid]
            new_node.keys = child.keys[mid + 1:]
            new_node.children = child.children[mid + 1:]
            for moved_child in new_node.children:
                moved_child.parent = new_node
            child.keys = child.keys[:mid]
            child.children = child.children[:mid + 1]

            parent.keys.insert(index, promote_key)
            parent.children.insert(index + 1, new_node)

    # Delete a key and rebalance locally so the operation remains O(log n).
    def delete(self, key):
        leaf = self._find_leaf(key)
        idx = bisect_left(leaf.keys, key)
        if idx >= len(leaf.keys) or leaf.keys[idx] != key:
            return False

        self._delete_from_leaf(leaf, idx)
        return True

    # Remove an item from a leaf and trigger underflow fixes if needed.
    def _delete_from_leaf(self, leaf, key_index):
        removed_first_key = key_index == 0
        leaf.keys.pop(key_index)
        leaf.values.pop(key_index)

        if leaf is self.root:
            return

        # If the first key changed, its parent separator may need an update.
        if removed_first_key and leaf.keys:
            self._propagate_min_change(leaf, leaf.keys[0])

        if len(leaf.keys) < self.t - 1:
            self._fix_underflow(leaf)

    # Walk up until we find the separator that references this subtree's minimum.
    def _propagate_min_change(self, node, new_min):
        current = node
        while current.parent is not None:
            parent = current.parent
            idx = parent.children.index(current)
            if idx > 0:
                parent.keys[idx - 1] = new_min
                return
            current = parent

    # Rebalance an underfull node by borrow-first, then merge, then recurse upward.
    def _fix_underflow(self, node):
        while node is not self.root and len(node.keys) < self.t - 1:
            parent = node.parent
            idx = parent.children.index(node)
            left_sibling = parent.children[idx - 1] if idx > 0 else None
            right_sibling = parent.children[idx + 1] if idx + 1 < len(parent.children) else None

            if left_sibling is not None and len(left_sibling.keys) > self.t - 1:
                if node.leaf:
                    self._borrow_from_left_leaf(node, left_sibling, parent, idx)
                    self._propagate_min_change(node, node.keys[0])
                else:
                    moved_min = self._borrow_from_left_internal(node, left_sibling, parent, idx)
                    self._propagate_min_change(node, moved_min)
                return

            if right_sibling is not None and len(right_sibling.keys) > self.t - 1:
                old_first = node.keys[0] if node.keys else None
                if node.leaf:
                    self._borrow_from_right_leaf(node, right_sibling, parent, idx)
                    if node.keys and node.keys[0] != old_first:
                        self._propagate_min_change(node, node.keys[0])
                else:
                    self._borrow_from_right_internal(node, right_sibling, parent, idx)
                return

            if left_sibling is not None:
                if node.leaf:
                    self._merge_leaf_nodes(left_sibling, node, parent, idx - 1)
                else:
                    self._merge_internal_nodes(left_sibling, node, parent, idx - 1)
                node = parent
                continue

            if right_sibling is None:
                break

            if node.leaf:
                old_first = node.keys[0] if node.keys else None
                self._merge_leaf_nodes(node, right_sibling, parent, idx)
                if node.keys and node.keys[0] != old_first:
                    self._propagate_min_change(node, node.keys[0])
            else:
                self._merge_internal_nodes(node, right_sibling, parent, idx)
            node = parent

        if not self.root.leaf and len(self.root.keys) == 0:
            self.root = self.root.children[0]
            self.root.parent = None

    # Borrow the largest key/value from the left leaf sibling.
    def _borrow_from_left_leaf(self, node, left_sibling, parent, idx):
        node.keys.insert(0, left_sibling.keys.pop())
        node.values.insert(0, left_sibling.values.pop())
        parent.keys[idx - 1] = node.keys[0]

    # Borrow the smallest key/value from the right leaf sibling.
    def _borrow_from_right_leaf(self, node, right_sibling, parent, idx):
        node.keys.append(right_sibling.keys.pop(0))
        node.values.append(right_sibling.values.pop(0))
        parent.keys[idx] = right_sibling.keys[0]

    # Merge two adjacent leaves into left_node and unlink right_node.
    def _merge_leaf_nodes(self, left_node, right_node, parent, separator_index):
        left_node.keys.extend(right_node.keys)
        left_node.values.extend(right_node.values)
        left_node.next = right_node.next

        parent.keys.pop(separator_index)
        parent.children.pop(separator_index + 1)
        right_node.parent = None

    # Borrow one child from the left internal sibling.
    def _borrow_from_left_internal(self, node, left_sibling, parent, idx):
        separator = parent.keys[idx - 1]
        moved_min = left_sibling.keys.pop()
        moved_child = left_sibling.children.pop()

        node.children.insert(0, moved_child)
        moved_child.parent = node
        node.keys.insert(0, separator)
        parent.keys[idx - 1] = moved_min
        return moved_min

    # Borrow one child from the right internal sibling.
    def _borrow_from_right_internal(self, node, right_sibling, parent, idx):
        separator = parent.keys[idx]
        moved_child = right_sibling.children.pop(0)

        node.children.append(moved_child)
        moved_child.parent = node
        node.keys.append(separator)
        parent.keys[idx] = right_sibling.keys.pop(0)

    # Merge two adjacent internal nodes into left_node and remove parent separator.
    def _merge_internal_nodes(self, left_node, right_node, parent, separator_index):
        separator = parent.keys.pop(separator_index)
        parent.children.pop(separator_index + 1)

        left_node.keys.append(separator)
        left_node.keys.extend(right_node.keys)
        for child in right_node.children:
            child.parent = left_node
            left_node.children.append(child)
        right_node.parent = None

    # Replace the stored value for an existing key.
    def update(self, key, new_value):
        leaf = self._find_leaf(key)
        idx = bisect_left(leaf.keys, key)
        if idx < len(leaf.keys) and leaf.keys[idx] == key:
            leaf.values[idx] = new_value
            return True
        return False

    # Return all key-value pairs with keys inside the inclusive range.
    def range_query(self, start_key, end_key):
        if start_key > end_key:
            return []

        node = self._find_leaf(start_key)
        result = []
        while node:
            for idx, key in enumerate(node.keys):
                if key < start_key:
                    continue
                if key > end_key:
                    return result
                result.append((key, node.values[idx]))
            node = node.next
        return result

    # Return every key-value pair in sorted key order.
    def get_all(self):
        node = self._leftmost_leaf()
        result = []
        while node:
            for idx, key in enumerate(node.keys):
                result.append((key, node.values[idx]))
            node = node.next
        return result

    # Serialize the full B+ tree shape so restart can rebuild identical nodes.
    def to_dict(self):
        def serialize_node(node):
            payload = {
                "leaf": node.leaf,
                "keys": list(node.keys),
            }
            if node.leaf:
                payload["values"] = [value for value in node.values]
            else:
                payload["children"] = [serialize_node(child) for child in node.children]
            return payload

        return {
            "t": self.t,
            "root": serialize_node(self.root),
        }

    # Restore a B+ tree from serialized JSON and reconnect leaf next pointers.
    @classmethod
    def from_dict(cls, payload):
        if not isinstance(payload, dict):
            raise TypeError("tree snapshot payload must be a dictionary")
        if "t" not in payload or "root" not in payload:
            raise ValueError("tree snapshot must contain 't' and 'root'")

        tree = cls(t=int(payload["t"]))
        leaves = []

        def build_node(node_payload, parent=None):
            if not isinstance(node_payload, dict):
                raise TypeError("tree node payload must be a dictionary")

            node = BPlusTreeNode(leaf=bool(node_payload.get("leaf", False)))
            node.parent = parent
            node.keys = list(node_payload.get("keys", []))

            if node.leaf:
                node.values = list(node_payload.get("values", []))
                if len(node.keys) != len(node.values):
                    raise ValueError("leaf node keys/values length mismatch in snapshot")
                leaves.append(node)
                return node

            child_payloads = node_payload.get("children", [])
            if not isinstance(child_payloads, list):
                raise TypeError("internal node children must be a list")
            node.children = [build_node(child_payload, parent=node) for child_payload in child_payloads]
            return node

        tree.root = build_node(payload["root"])

        for index in range(len(leaves) - 1):
            leaves[index].next = leaves[index + 1]

        return tree

    # Build and optionally render a Graphviz visualization of the current tree.
    def visualize_tree(self, as_figure=True):
        try:
            from graphviz import Digraph
        except ImportError as exc:
            raise ImportError(
                "graphviz is required for tree visualization. Install with 'pip install graphviz'."
            ) from exc

        dot = Digraph("BPlusTree")
        dot.attr(rankdir="TB", splines="polyline")
        dot.attr(
            "node",
            shape="box",
            style="rounded",
            fontname="Helvetica",
            color="#333333",
        )
        self._add_nodes(dot, self.root)
        self._add_edges(dot, self.root)

        # When the root is also a leaf, include a visual root so the output still looks like a tree.
        if self.root.leaf:
            root_id = "virtual_root"
            dot.node(root_id, "Root", shape="circle", style="filled", fillcolor="#e8f1ff")
            dot.edge(root_id, str(id(self.root)))

        if not as_figure:
            return dot

        try:
            from IPython.display import Image
        except ImportError as exc:
            raise ImportError(
                "IPython is required to display tree figures inline. Install with 'pip install ipython'."
            ) from exc

        image_data = dot.pipe(format="png")
        return Image(data=image_data)

    # Escape text so it is safe to embed inside Graphviz node labels.
    def _escape_label_text(self, text):
        return str(text).replace("\\", "\\\\").replace("\n", "\\n")

    # Add Graphviz nodes recursively for all internal and leaf tree nodes.
    def _add_nodes(self, dot, node):
        node_id = str(id(node))
        if node.leaf:
            if node.keys:
                pairs = [
                    f"{self._escape_label_text(k)}: {self._escape_label_text(node.values[i])}"
                    for i, k in enumerate(node.keys)
                ]
                label = "Leaf\\n" + "\\n".join(pairs)
            else:
                label = "Leaf\\nempty"
        else:
            if node.keys:
                label = "Internal\\nkeys: " + ", ".join(self._escape_label_text(k) for k in node.keys)
            else:
                label = "Internal\\nroot"

        dot.node(node_id, label)
        if not node.leaf:
            for child in node.children:
                self._add_nodes(dot, child)

    # Add Graphviz edges for child links and dashed leaf-level next pointers.
    def _add_edges(self, dot, node):
        node_id = str(id(node))
        if node.leaf:
            if node.next is not None:
                dot.edge(node_id, str(id(node.next)), style="dashed", color="gray", constraint="false")
            return

        for child in node.children:
            child_id = str(id(child))
            dot.edge(node_id, child_id)
            self._add_edges(dot, child)