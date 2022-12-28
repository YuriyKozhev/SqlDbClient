class ParameterNotSpecifiedException(Exception):
    def __init__(self, parameter: str):
        super().__init__(f'{parameter} parameter should be specified')
