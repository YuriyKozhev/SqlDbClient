class NotInTransActionException(Exception):
    """Exception to raise when trying to commit outside a transaction"""
    def __init__(self):
        super(NotInTransActionException, self).__init__('Not in transaction')
