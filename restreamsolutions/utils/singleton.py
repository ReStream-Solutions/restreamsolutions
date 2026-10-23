class Singleton(type):
    """Metaclass that creates a single shared instance per class."""

    _instances = {}

    def __call__(cls, *args, **kwargs):
        """Return the shared instance of the class, creating it on the first call.

        Args:
            *args, **kwargs: Constructor arguments, used only when the instance is created.

        Returns:
            The single instance of the class.
        """
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]
