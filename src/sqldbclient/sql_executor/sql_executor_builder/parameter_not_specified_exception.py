class ParameterNotSpecifiedException(Exception):
    """Exception to raise when mandatory parameter is not specified"""
    def __init__(self, parameter: str):
        super().__init__(f'{parameter} parameter should be specified')
