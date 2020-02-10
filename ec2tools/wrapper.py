import boto3
from .ec2tools import get, create_filters

ec2 = boto3.client("ec2")
ec2_res = boto3.resource("ec2")


def describe_volumes(volume_ids=None, filters=None, path="$.[*]"):
    """
    Simpler access to describe_volumes().
    
    Args:
        volume_ids (str|list): ids to query
        filters (dict): query filters
        path (str): JSONpath query
    Returns:
        object: JSON-like
        
    
    """
    kwargs = {}

    if volume_ids:
        if not hasattr(volume_ids, "__len__"):
            volume_ids = [volume_ids]
        kwargs["VolumeIds"] = volume_ids

    if filters:
        kwargs["Filters"] = filters

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
    """
    volumes = [
        ec2_res.Volume(v)
        for v in describe_volumes(
            volume_ids, create_filters(**filters), path="[*].VolumeId"
        )
    ]
    return volumes


def describe_instances(instance_ids=None, filters=None, path="$.[*]"):
    """
    Simpler access to describe_instances().
    
    Args:
        instance_ids (str|list): ids to query
        filters (dict): query filters
        path (str): JSONpath query
    Returns:
        object: JSON-like
    """
    kwargs = {}

    if instance_ids:
        if not hasattr(instance_ids, "__len__"):
            instance_ids = [instance_ids]
        kwargs["InstanceIds"] = instance_ids

    if filters:
        kwargs["Filters"] = filters

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
    """
    instances = [
        ec2_res.Instance(v)
        for v in describe_instances(
            instance_ids, create_filters(**filters), path="[*].Instances[*].InstanceId"
        )
    ]
    return instances
