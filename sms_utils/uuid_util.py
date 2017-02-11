import uuid


def generate_namespace(name):
    if not isinstance(name, str):
        raise ValueError("name field should be of type str. {type} provided".format(type=type(name)))
    return uuid.uuid5(namespace=uuid.NAMESPACE_DNS, name=name)


def generate_uuid(namespace_str, key_str):
    if not isinstance(namespace_str, str) or not isinstance(key_str, str):
        raise ValueError(
            "namespace_str and key_str field should be of type str. {namespace} and {key} provided".format(
                namespace=type(namespace_str), key=type(key_str)))
    return uuid.uuid5(namespace=generate_namespace(name=namespace_str), name=key_str)
