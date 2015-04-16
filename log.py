#==========================================================================#
# Log Entry
#==========================================================================#

class logEntry(object):
    def __init__(self, transaction, key, old, new):
        self.key = key
        self.old = old
        self.new = new
        self.transaction = transaction

    def printable(self):
        return "%s|%s|%s|%s\n" %(self.transaction, self.key, self.old, self.new)

def log(transaction, key, old, new):
    return logEntry(transaction, key, old, new)


