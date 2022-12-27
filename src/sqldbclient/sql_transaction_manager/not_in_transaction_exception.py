class NotInTransActionException(Exception):
    def __init__(self):
        super(NotInTransActionException, self).__init__('Not in transaction')
