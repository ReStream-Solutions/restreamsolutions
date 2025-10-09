from datastore_sdk.base_interface import BaseInterface


def check_instance_types(obj: BaseInterface):
    for attr_name, attr_type in obj._hints.items():
        assert hasattr(obj, attr_name)
        if getattr(obj, attr_name) is None:
            continue
        assert isinstance(getattr(obj, attr_name), attr_type)
