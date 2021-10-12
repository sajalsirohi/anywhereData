class Singleton(type):
    """
    __metaclass__ to force singleton behavior for classes that use this class as metaclass
    """
    # dict to maintain the instances, to keep track of classes that have to only initialized
    # once. This variable is shared between the classes that are singleton by using
    # this class as metaclass.
    _instances = {}

    # __call__ method is 'inherited' in 'base' classes. my_derived_class.__call__()
    # is indirectly this method.
    def __call__(cls, *args, **kwargs):
        # if we have already initialized that class
        if cls not in cls._instances:
            # this returns an instance of 'cls' and then calls '__call__' method on
            # that instance.
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]
