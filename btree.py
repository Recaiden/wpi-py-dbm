#!/usr/bin/env python

import bisect
import itertools
import operator

#==========================================================================#
# Nodes
#==========================================================================#
class bpnode(object):
    '''Node that is used as the interior/branches of a b+ tree, 
    containing only keys, no values'''
    def __init__(self, tree, keys=None, children=None):
        self.tree = tree
        # no values, only keys
        self.keys = keys or []
        self.children =  children or []
        
        self.leaf = False

    def lateral(self, parent, idxParent, dest, dest_idx):
        '''Transfer keys to an adjacent sibling node for resizing'''
        # Test the index against the parent to determine which sibling to change
        if idxParent > dest_idx:
            dest.keys.append(parent.keys[dest_idx])
            parent.keys[dest_idx] = self.keys.pop(0)
            if self.children:
                dest.children.append(self.children.pop(0))
        else:
            dest.keys.insert(0, parent.keys[idxParent])
            parent.keys[idxParent] = self.keys.pop()
            if self.children:
                dest.children.insert(0, self.children.pop())

    def shrink(self, ancestors):
        parent = None

        if ancestors:
            parent, idxParent = ancestors.pop()
            # Prefer left sibling if possible
            if idxParent:
                sibLeft = parent.children[idxParent - 1]
                if len(sibLeft.keys) < self.tree.fanout:
                    self.lateral(parent, idxParent, sibLeft, idxParent - 1)
                    return

            # Right Sibling
            if idxParent + 1 < len(parent.children):
                sibRight = parent.children[idxParent + 1]
                if len(sibRight.keys) < self.tree.fanout:
                    self.lateral(parent, idxParent, sibRight, idxParent + 1)
                    return
                
        center = len(self.keys) / 2
        sibling, push = self.split()

        if not parent:
            parent, idxParent = bpnode(self.tree, children=[self]), 0
            self.tree._root = parent

        # pass the median up to the parent
        parent.keys.insert(idxParent, push)
        parent.children.insert(idxParent + 1, sibling)
        if len(parent.keys) > parent.tree.fanout:
            parent.shrink(ancestors)

    def grow(self, ancestors):
        parent, idxParent = ancestors.pop()

        minimum = self.tree.fanout / 2
        sibLeft = sibRight = None
        
        # try to borrow from the right sibling
        if idxParent + 1 < len(parent.children):
            sibRight = parent.children[idxParent + 1]
            if len(sibRight.keys) > minimum:
                sibRight.lateral(parent, idxParent + 1, self, idxParent)
                return

        # try to borrow from the left sibling
        if idxParent:
            sibLeft = parent.children[idxParent - 1]
            if len(sibLeft.keys) > minimum:
                sibLeft.lateral(parent, idxParent - 1, self, idxParent)
                return

        # consolidate with a sibling - try left first
        if sibLeft:
            sibLeft.keys.append(parent.keys[idxParent - 1])
            sibLeft.keys.extend(self.keys)
            if self.children:
                sibLeft.children.extend(self.children)
                parent.keys.pop(idxParent - 1)
                parent.children.pop(idxParent)
        else:
            self.keys.append(parent.keys[idxParent])
            self.keys.extend(sibRight.keys)
            if self.children:
                self.children.extend(sibRight.children)
                parent.keys.pop(idxParent)
                parent.children.pop(idxParent + 1)
                
        if len(parent.keys) < minimum:
            if ancestors:
                # parent is not the root
                parent.grow(ancestors)
            elif not parent.keys:
                # parent is root, and its now empty
                self.tree._root = sibLeft or self

    def split(self):
        center = len(self.keys) / 2
        median = self.keys[center]
        sibling = type(self)(self.tree, self.keys[center + 1:], self.children[center + 1:])
        self.keys = self.keys[:center]
        self.children = self.children[:center + 1]
        return sibling, median
    
    def insert(self, idx, item, ancestors):
        self.keys.insert(idx, item)
        if len(self.keys) > self.tree.fanout:
            self.shrink(ancestors)
            
    def remove(self, idx, ancestors):
        minimum = self.tree.fanout / 2
        
        if self.children:
            # find the smallest in the right subtree, exchange the value with the current nodes
            # then delete the smallest one, just like the idea in the binary search tree.
            # Note: only if len(descendent.keys) > minimum, we do this way in fanout to avoid 'grow' operation.
            # Or we will inspect the left tree and do it any way
            # all internal nodes have both left and right subtree.
            additional_ancestors = [(self, idx + 1)]
            descendent = self.children[idx + 1]
            while descendent.children:
                additional_ancestors.append((descendent, 0))
                descendent = descendent.children[0]
            if len(descendent.keys) > minimum:
                ancestors.extend(additional_ancestors)
                self.keys[idx] = descendent.keys[0]
                descendent.remove(0, ancestors)
                return
            
            # fall back to the left child, and exchange with the biggest, then delete the biggest anyway.
            additional_ancestors = [(self, idx)]
            descendent = self.children[idx]
            while descendent.children:
                additional_ancestors.append(
                    (descendent, len(descendent.children) - 1))
                descendent = descendent.children[-1]
                ancestors.extend(additional_ancestors)
                self.keys[idx] = descendent.keys[-1]
                descendent.remove(len(descendent.children) - 1, ancestors)
        else:
            self.keys.pop(idx)
            if len(self.keys) < minimum and ancestors:
                self.grow(ancestors)
                
class bpleaf(bpnode):
    # can't use slots because it would break pickling?
    '''
    '''
    def __init__(self, tree, keys=None, vals=None, link=None):
        self.tree = tree
        # Parallel arrays of keys and value.  For leaves, always modified together
        self.keys = keys or []
        self.vals = vals or []

        # Connection to next leaf
        self.link = link
        
        self.leaf = True

    #==========================================================================#
    # Internal methods
    #==========================================================================#
    def fullness(self):
        return len(self.keys)

    def insert(self, idx, key, value, ancestors):
        # Insert the key and value to matching positions in the lists
        self.keys.insert(idx, key)
        self.vals.insert(idx, value)

        if len(self.keys) > self.tree.fanout:
            self.shrink(ancestors)

    def lateral(self, parent, idxParent, dst, idxDst):
        '''Move an element to a sibling node to rebalance the tree'''
        # Transfer first item to the left sibling node
        if idxParent > idxDst:
            dst.keys.append(self.keys.pop(0))
            dst.vals.append(self.vals.pop(0))
            parent.keys[idxDst] = self.keys[0]
        # Transfer last item to the right sibling node
        else:
            dst.keys.insert(self.keys.pop())
            dst.vals.insert(self.vals.pop())
            parent.keys[idxParent] = dst.keys[0]
            
    def split(self):
        '''Divide self in two, creating a sibling to our right'''
        idxCenter = len(self.keys) / 2 # integer division, round down
        median = self.keys[idxCenter - 1]
        
        sibRight = type(self)(self.tree, self.keys[idxCenter:], self.vals[idxCenter:], self.link)
        self.keys = self.keys[:idxCenter]
        self.vals = self.vals[:idxCenter]
        self.link = sibRight
        
        return sibRight, sibRight.keys[0]
    
    def remove(self, idx, ancestors):
        minimum = self.tree.fanout / 2
        if idx >= len(self.keys):
            self = self.link
            idx = 0

        key = self.keys[idx]
        current = self

        if not ancestors:
            current.keys.pop(idx)
            current.vals.pop(idx)
            return
        
        # Avoid rebalancing if possible
        while current is not None and current.keys[0] == key:
            if len(current.keys) > minimum:
                if current.keys[0] == key:
                    idx = 0
                else:
                    idx = bisect.bisect_left(current.keys, key)
                current.keys.pop(idx)
                current.vals.pop(idx)
                return
            current = current.link

        self.grow(ancestors)

                
    def grow(self, ancestors):
        minimum = self.tree.fanout / 2
        parent, idxParent = ancestors.pop()
        sibLeft = sibRight = None

        # try borrowing from a neighbor - try right first
        if idxParent + 1 < len(parent.children):
            sibRight = parent.children[idxParent + 1]
            if len(sibRight.keys) > minimum:
                sibRight.lateral(parent, idxParent + 1, self, idxParent)
                return

        # fallback to left
        if idxParent:
            sibLeft = parent.children[idxParent - 1]
            if len(sibLeft.keys) > minimum:
                sibLeft.lateral(parent, idxParent - 1, self, idxParent)
                return

        # join with a neighbor - try left first
        if sibLeft:
            sibLeft.keys.extend(self.keys)
            sibLeft.vals.extend(self.vals)
            parent.remove(idxParent - 1, ancestors)
            return
        
        # fallback to right
        self.keys.extend(sibRight.keys)
        self.vals.extend(sibRight.vals)
        parent.remove(idxParent, ancestors)

    def shrink(self, ancestors):
        parent = None

        if ancestors:
            parent, idxParent = ancestors.pop()
            # try to lend to the left neighboring sibling
            if idxParent:
                sibLeft = parent.children[idxParent - 1]
                if len(sibLeft.keys) < self.tree.fanout:
                    self.lateral(parent, idxParent, sibLeft, idxParent - 1)
                    return

            # try the right neighbor
            if idxParent + 1 < len(parent.children):
                sibRight = parent.children[idxParent + 1]
                if len(sibRight.keys) < self.tree.fanout:
                    self.lateral(parent, idxParent, sibRight, idxParent + 1)
                    return

        center = len(self.keys) / 2
        sibling, push = self.split()
        
        if not parent:
            parent = bpnode(self.tree, children=[self])
            idxParent = 0
            self.tree._root = parent

        # pass the median up to the parent
        parent.keys.insert(idxParent, push)
        parent.children.insert(idxParent + 1, sibling)
        if len(parent.keys) > parent.tree.fanout:
            parent.shrink(ancestors)

#==========================================================================#
# Tree
#==========================================================================#
class bptree(object):
    def __init__(self, fanout=50):
        self.fanout = fanout
        self._root = self._bottom = bpleaf(self)

    def _get(self, key):
        node, idx = self._path_to(key)[-1]

        if idx == len(node.keys):
            if node.link:
                node, idx = node.link, 0
            else:
                return

        while node.keys[idx] == key:
            yield node.vals[idx]
            idx += 1
            if idx == len(node.keys):
                if node.link:
                    node, idx = node.link, 0
                else:
                    return

    def _path_to_interior(self, item):
        current = self._root
        ancestry = []

        while getattr(current, "children", None):
            idx = bisect.bisect_left(current.keys, item)
            ancestry.append((current, idx))
            if idx < len(current.keys) and current.keys[idx] == item:
                return ancestry
            current = current.children[idx]
            
        idx = bisect.bisect_left(current.keys, item)
        ancestry.append((current, idx))
        present = idx < len(current.keys)
        present = present and current.keys[idx] == item
        return ancestry

    def _path_to(self, item):
        path = self._path_to_interior(item)
        node, idx = path[-1]
        while hasattr(node, "children"):
            node = node.children[idx]
            idx = bisect.bisect_left(node.keys, item)
            path.append((node, idx))
        return path

    #==========================================================================#
    # Indexing methods
    #==========================================================================#
    # Allow the user to interact with the b+tree using [] rather than methods
    def get(self, key, default=None):
        try:
            return self._get(key).next()
        except StopIteration:
            return default

    def getlist(self, key):
        return list(self._get(key))

    def insert(self, key, value):
        path = self._path_to(key)
        node, idx = path.pop()
        node.insert(idx, key, value, path)

    def remove(self, key):
        path = self._path_to(key)
        node, idx = path.pop()
        print "path:", path, "Node:", node, "idx:", idx
        node.remove(idx, path)
    # Define index operations on the tree class [i]
    __getitem__ = get
    __setitem__ = insert
    __delitem__ = remove

    def __contains__(self, key):
        return item in self._get(key)

    #==========================================================================#
    # Iteration methods
    #==========================================================================#
    # Collectively, allow a user to interact with the b+ tree as if it were a dictionary
    def iteritems(self):
        node = self._root
        while hasattr(node, "children"):
            node = node.children[0]

        while node:
            for pair in itertools.izip(node.keys, node.vals):
                yield pair
            node = node.link

    def iterkeys(self):
        return itertools.imap(operator.itemgetter(0), self.iteritems())

    def itervalues(self):
        return itertools.imap(operator.itemgetter(1), self.iteritems())

    __iter__ = iterkeys

    def items(self): return list(self.iteritems())
    def keys(self): return list(self.iterkeys())
    def values(self): return list(self.itervalues())
    
if __name__ == '__main__':
    print "Class for b+ trees.  Do not invoke directly."

