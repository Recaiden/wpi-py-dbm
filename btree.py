#!/usr/bin/env python

FANOUT = 100

class bptree(object):
    pass


#==========================================================================#
# Facing methods
#==========================================================================#
# put/get/remove that use insert/update/search/delete?
def put(key, value, root):
    pass
def get(key, root):
    pass


def remove(key, root):
    delete(key, root)
    
class bpnode(object):
    '''Each node contains a list of keys.
    The child 'beside'/'between' those keys will be the part of the tree that holds them.
    If a node is a leaf it will also hold a group of values and a link to the next node.
    '''
    def __init__(self, root, kay, value):
        self.children = []
        self.keys = []
        self.parent = root
        
        self.leaf = False
        self.next = None
        
        # This is a new tree
        if root is None:
            pass
        else:
            pass

    def fullness(self):
        return len(self.keys)        
            

    #==========================================================================#
    # Internal methods
    #==========================================================================#
    def insert(self, key, value, node):
        nodeToAddTo = self.search(key, node)
        
        pass

    def search(self, key, node):
        # Check what level we're at
        if node.leaf:
            return node

        # Move along until the pointer variable index is at the right chunk, then recurse
        index = 0
        while key > node.keys[index]:
            index += 1
        return search(key, node.children[index])

    def delete(self, key):
        pass
