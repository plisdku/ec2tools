import boto3
from .ec2tools import get, create_filters

ec2 = boto3.client("ec2")
ec2_res = boto3.resource("ec2")


def _to_list(scalar_or_list):
    """
    Adapter to map None -> None, scalar -> [scalar], list -> list.
    """
    if scalar_or_list is None:
        return None
    if isinstance(scalar_or_list, str):
        return [scalar_or_list]
    if not hasattr(scalar_or_list, "__len__"):
        return [scalar_or_list]
    return scalar_or_list


def _describe_kwargs(id_field_name, ids=None, filters=None):
    """
    Get kwargs for an ec2.describe_* function.
    
    The filters dict will be passed to create_filters.  Check the docstring for create_filters.
    
    Args:
        id_field_name (str): something plural like "VolumeIds"
        ids (None|str|list): list of ids (volume ids, instance ids)
        filters (dict): valid filters in create_filters form, e.g. {"size":"8"}
    Returns:
        dict: kwargs for some ec2.describe_ function.
    """
    kwargs = {}
    ids = _to_list(ids)

    if ids:
        kwargs[id_field_name] = ids

    if filters:
        kwargs["Filters"] = create_filters(**filters)

    return kwargs


def describe_volumes(volume_ids=None, path="$[*]", **filters):
    """
    Simpler access to describe_volumes().
    
    Args:
        volume_ids (str|list): ids to query
        path (str): JSONpath query
        filters (dict): query filters
    Returns:
        object: JSON-like
    """
    kwargs = _describe_kwargs("VolumeIds", volume_ids, filters)
    v = ec2.describe_volumes(**kwargs)
    return get(v["Volumes"], path)


def get_volumes(volume_ids=None, **filters):
    """
    Get list of volumes.
    
    Args:
        volume_ids (str|list): ids to query
        filters (dict): query filters
    Returns:
        list: boto3.resources.factory.ec2.Volume objects

    Example:
        >>> get_volumes(size="8") # filter for size
        >>> get_volumes(volume_ids="vol-0abec2c0447f5bbb8") # get particular volume
        >>> get_volumes(volume_id="vol*") # filter volume IDs with wildcard
    """
    volumes = [
        ec2_res.Volume(v)
        for v in describe_volumes(volume_ids, "[*].VolumeId", **filters)
    ]
    return volumes


def describe_instances(instance_ids=None, path="$.[*]", **filters):
    """
    Simpler access to describe_instances().
    
    Args:
        instance_ids (str|list): ids to query
        path (str): JSONpath query
        filters (dict): query filters
    Returns:
        object: JSON-like
    """

    kwargs = _describe_kwargs("InstanceIds", instance_ids, filters)
    v = ec2.describe_instances(**kwargs)
    return get(v["Reservations"], path)


def get_instances(instance_ids=None, **filters):
    """
    Get list of instances.
    
    Args:
        instance_ids (str|list): ids to query
        filters (dict): query filters
    Returns:
        list: boto3.resources.factory.ec2.Instance objects

    Example:
    
        # Get IDs for instances with name matching the "myInstance*" pattern
        >>> describe_instances(path="[*].Instances[*].InstanceId", tag_Name="myInstance*")
    """
    instances = [
        ec2_res.Instance(v)
        for v in describe_instances(
            instance_ids, path="[*].Instances[*].InstanceId", **filters
        )
    ]
    return instances


def describe_key_pairs(key_pair_ids=None, path="$.[*]", **filters):
    """
    Simpler access to describe_key_pairs().
    
    Args:
        key_pair_ids (str|list): ids to query
        path (str): JSONpath query
        filters (dict): valid filters in create_filters form, e.g. {"size":"8"}
    Returns:
        object: JSON-like
    """
    kwargs = _describe_kwargs("KeyPairIds", key_pair_ids, filters)
    v = ec2.describe_key_pairs(**kwargs)
    return get(v["KeyPairs"], path)


def get_key_pairs(key_pair_ids=None, **filters):
    """
    Get list of key pairs.
    
    Args:
        key_pair_ids (str|list): ids to query
        filters (dict): query filters
    Returns:
        list: boto3.resources.factory.ec2.KeyPair objects
    """
    key_pairs = [
        ec2_res.KeyPair(v)
        for v in describe_key_pairs(key_pair_ids, path="[*].KeyPairId", **filters)
    ]
    return key_pairs
