def update_dict_keys(obj, mapping_dict):
    """ utility function to replace existing keys with new set of keys recursively
    """
    if isinstance(obj, dict):
        return {mapping_dict[k]: update_dict_keys(v, mapping_dict) for k, v in obj.iteritems()}
    elif isinstance(obj, list):
        return [{mapping_dict[k]: update_dict_keys(v, mapping_dict) for k, v in item.iteritems()} for item in obj if
                isinstance(item, dict)]
    else:
        return obj
