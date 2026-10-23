from restreamsolutions.base_interface import BaseInterface


def check_instance_types(obj: BaseInterface):
    """Assert that every attribute of an SDK object that is set has the type declared in its type hints.

    Args:
        obj: The SDK object to check.
    """
    for attr_name, attr_type in obj._hints.items():
        if hasattr(obj, attr_name) and getattr(obj, attr_name) is not None:
            assert isinstance(getattr(obj, attr_name), attr_type)
