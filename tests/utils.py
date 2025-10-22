from datastore_sdk.base_interface import BaseInterface


def check_instance_types(obj: BaseInterface):
    for attr_name, attr_type in obj._hints.items():
        if hasattr(obj, attr_name) and getattr(obj, attr_name) is not None:
            assert isinstance(getattr(obj, attr_name), attr_type)
