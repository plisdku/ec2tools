import boto3
from ec2tools import get

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


def get_volumes(volume_ids=None, filters=None):
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
        for v in describe_volumes(volume_ids, filters, path="[*].VolumeId")
    ]
    return volumes
