class Singleton(type):
    _class_instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in Singleton._class_instances:
            Singleton._class_instances[cls] = super().__call__(*args, **kwargs)
        return Singleton._class_instances[cls]
