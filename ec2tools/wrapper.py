import boto3
from .ec2tools import get

ec2 = boto3.client("ec2")
ec2_res = boto3.resource("ec2")


def _translate_filters(**kw_filters):
    """
    Translate dict keys to valid EC2 key names.

    EC2 filters are key-values pairs where the key is lower case and hyphen-separated,
    and the values are a list of strings.  This function will
    1. convert underscore-separated Python kwargs to hyphen-separated strings for EC2,
      1b. any key of the form "tag_*" other than "tag_key" will be converted into the special
          filter string "tag:*".  If the key does not start with "tag_" don't worry about this...
    2. if a single string value is given, creates a one-element list containing the string
    3. creates a suitable JSON-like dict for boto3 filters.

    Args:
        **kw_filters: filter key-value pairs
    Returns:
        dict: filters with translated keys
    
    Examples:
    
        >>> _translate_filters(resource_type="instance")
        [{'Name': 'resource-type', 'Values': ['instance']}]
        
        >>> _translate_filters(tag_Name="instance")
        [{'Name': 'tag:Name', 'Values': ['instance']}]
    
    EC2 commands each have their own valid filter names!  Check on the command line, e.g.
    
        >>> aws2 ec2 describe-instances help
        
        >>> aws2 ec2 describe-images help
    
    and so on to see lists of valid filter names.
    """
    out = {}
    for key, value in kw_filters.items():
        if key.startswith("tag_") and key != "tag_key":
            key = key.replace("_", ":")
        else:
            key = key.replace("_", "-")
        out[key] = value
    return out


def _create_ec2_filters(filters):
    """
    Quick filter creation for describe-images, describe-instances, etc.
    
    Args:
        filters: key-value 
    Returns:
        list: JSON-like EC2 Filters object
    """
    ec2_filters = []
    for key, value in filters.items():

        # Translate bools
        if isinstance(value, bool):
            if value:
                value = "true"
            else:
                value = "false"

        # I don't know if this is a universal behavior, but describe_images has a filter
        # called is-public that must be a lower-case string in ["true", "false"].
        if key.startswith("is-") and value in ["True", "False"]:
            value = value.lower()

        if not isinstance(value, list):
            value = [value]

        ec2_filters.append({"Name": key, "Values": value})

    return ec2_filters


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


def _describe_kwargs(id_field_name, ids=None, filters=None, kw_filters=None):
    """
    Get kwargs for an ec2.describe_* function.
    
    The filters dict will be passed to create_filters.  Check the docstring for create_filters.
    
    Args:
        id_field_name (str): something plural like "VolumeIds"
        ids (None|str|list): list of ids (volume ids, instance ids)
        filters (dict): valid filters in EC2 form, e.g. {"egress.ip-permission.cidr":"*"}
        kw_filters (dict): valid filters in create_filters form, e.g. {"size":"8"}
    Returns:
        dict: kwargs for some ec2.describe_ function.
    """
    kwargs = {}
    ids = _to_list(ids)

    if ids:
        kwargs[id_field_name] = ids

    all_filters = {}
    if kw_filters:
        all_filters = _translate_filters(**kw_filters)
    if filters:
        all_filters.update(filters)
    if all_filters:
        kwargs["Filters"] = _create_ec2_filters(all_filters)

    return kwargs


def describe_volumes(volume_ids=None, path="$[*]", filters=None, **kw_filters):
    """
    Simpler access to describe_volumes().
    
    Args:
        volume_ids (str|list): ids to query
        path (str): JSONpath query
        filters (dict): EC2 filters, e.g. {"attachment.delete-on-termination":True}
        kw_filters (dict): valid filters in create_filters form, e.g. size="8"
    Returns:
        object: JSON-like
    """
    kwargs = _describe_kwargs("VolumeIds", volume_ids, filters, kw_filters)
    v = ec2.describe_volumes(**kwargs)
    return get(v["Volumes"], path)


def get_volumes(volume_ids=None, filters=None, **kw_filters):
    """
    Get list of volumes.
    
    Args:
        volume_ids (str|list): ids to query
        filters (dict): EC2 filters, e.g. {"attachment.delete-on-termination":True}
        kw_filters (dict): filters in create_filters form, e.g. size="8"
    Returns:
        list: boto3.resources.factory.ec2.Volume objects

    Example:
        >>> get_volumes(size="8") # filter for size
        >>> get_volumes(volume_ids="vol-0abec2c0447f5bbb8") # get particular volume
        >>> get_volumes(volume_id="vol*") # filter volume IDs with wildcard
    """
    volumes = [
        ec2_res.Volume(v)
        for v in describe_volumes(volume_ids, "[*].VolumeId", filters, **kw_filters)
    ]
    return volumes


def describe_instances(instance_ids=None, path="$.[*]", filters=None, **kw_filters):
    """
    Simpler access to describe_instances().
    
    Args:
        instance_ids (str|list): ids to query
        path (str): JSONpath query
        filters (dict):  EC2 filters, e.g. {"block-device-mapping.device-name":"/dev/xvda"}
        **kw_filters: filters in create_filters form, e.g. instance_type="t2.micro"
    Returns:
        object: JSON-like
    """

    kwargs = _describe_kwargs("InstanceIds", instance_ids, filters, kw_filters)
    v = ec2.describe_instances(**kwargs)
    return get(v["Reservations"], path)


def get_instances(instance_ids=None, filters=None, **kw_filters):
    """
    Get list of instances.
    
    Args:
        instance_ids (str|list): ids to query
        filters (dict):  EC2 filters, e.g. {"block-device-mapping.device-name":"/dev/xvda"}
        **kw_filters: filters in create_filters form, e.g. instance_type="t2.micro"
    Returns:
        list: boto3.resources.factory.ec2.Instance objects

    Example:
    
        # Get IDs for instances with name matching the "myInstance*" pattern
        >>> describe_instances(path="[*].Instances[*].InstanceId", tag_Name="myInstance*")
    """
    instances = [
        ec2_res.Instance(v)
        for v in describe_instances(
            instance_ids, "[*].Instances[*].InstanceId", filters, **kw_filters
        )
    ]
    return instances


def describe_key_pairs(key_pair_ids=None, path="$.[*]", filters=None, **kw_filters):
    """
    Simpler access to describe_key_pairs().
    
    Args:
        key_pair_ids (str|list): ids to query
        path (str): JSONpath query
        filters (dict):  EC2 filters, e.g. {"key-name":"admin-key-pair-us-west-1"}
        **kw_filters: filters in create_filters form, e.g. key_name="admin-key-pair-us-west-1"
    Returns:
        object: JSON-like
    """
    kwargs = _describe_kwargs("KeyPairIds", key_pair_ids, filters, kw_filters)

    v = ec2.describe_key_pairs(**kwargs)
    return get(v["KeyPairs"], path)


def get_key_pairs(key_pair_ids=None, filters=None, **kw_filters):
    """
    Get list of key pairs.
    
    Args:
        key_pair_ids (str|list): ids to query
        filters (dict):  EC2 filters, e.g. {"key-name":"admin-key-pair-us-west-1"}
        **kw_filters: filters in create_filters form, e.g. key_name="admin-key-pair-us-west-1"
    Returns:
        list: boto3.resources.factory.ec2.KeyPair objects
    """
    key_pairs = [
        ec2_res.KeyPair(v)
        for v in describe_key_pairs(
            key_pair_ids, "[*].KeyPairId", filters, **kw_filters
        )
    ]
    return key_pairs


def describe_security_groups(
    security_group_ids=None, path="$.[*]", filters=None, **kw_filters
):
    """
    Simpler access to describe_security_groups().
    
    Args:
        security_group_ids (str|list): ids to query
        path (str): JSONpath query
        filters (dict):  EC2 filters, e.g. {"ip-permission.from-port":"80"}
        **kw_filters: filters in create_filters form, e.g. group_name="admin_SG_us_west1"
    Returns:
        object: JSON-like
    """
    kwargs = _describe_kwargs(
        "SecurityGroupIds", security_group_ids, filters, kw_filters
    )
    v = ec2.describe_security_groups(**kwargs)
    return get(v["SecurityGroups"], path)


def get_security_groups(security_group_ids=None, filters=None, **kw_filters):
    """
    Get list of security groups.
    
    Args:
        security_group_ids (str|list): ids to query
        filters (dict):  EC2 filters, e.g. {"ip-permission.from-port":"80"}
        **kw_filters: filters in create_filters form, e.g. group_name="admin_SG_us_west1"
    Returns:
        list: boto3.resources.factory.ec2.SecurityGroup objects
    """
    security_groups = [
        ec2_res.SecurityGroup(v)
        for v in describe_security_groups(
            security_group_ids, "[*].GroupId", filters, **kw_filters
        )
    ]
    return security_groups


def describe_instance_types(
    instance_types=None, path="$.[*]", filters=None, **kw_filters
):
    """
    Simpler access to describe_instance_types().
    
    Args:
        instance_type_ids (str|list): ids to query
        path (str): JSONpath query
        filters (dict):  EC2 filters, e.g. {"ip-permission.from-port":"80"}
        **kw_filters: filters in create_filters form, e.g. group_name="admin_SG_us_west1"
    Returns:
        object: JSON-like
    """
    kwargs = _describe_kwargs("InstanceTypes", instance_types, filters, kw_filters)
    v = ec2.describe_instance_types(**kwargs)
    return get(v["InstanceTypes"], path)


# There is no get_instance_types because there is no InstanceType class.


def describe_images(
    executable_users=None,
    image_ids=None,
    owners=None,
    path="$.[*]",
    filters=None,
    **kw_filters
):
    """
    Simpler access to describe_images().
    
    Args:
        executable_users (str|list): AWS account ID, "self", or "all" (public AMIs)
        image_ids (str|list): ids to query
        owners (str|list): AWS account ID, "self", "amazon", "aws-marketplace", or "microsoft"
        path (str): JSONpath query
        filters (dict):  EC2 filters, e.g. {"is-public":"true"}
        **kw_filters: filters in create_filters form, e.g. architecture="x86_64"
    Returns:
        object: JSON-like
    """
    kwargs = _describe_kwargs("ImageIds", image_ids, filters, kw_filters)

    if executable_users is not None:
        kwargs.update({"ExecutableUsers": _to_list(executable_users)})
    if owners is not None:
        kwargs.update({"Owners": _to_list(owners)})

    v = ec2.describe_images(**kwargs)
    return get(v["Images"], path)


def get_images(
    executable_users=None, image_ids=None, owners=None, filters=None, **kw_filters
):
    """
    Get list of images.
    
    Args:
        executable_users (str|list): AWS account ID, "self", or "all" (public AMIs)
        image_ids (str|list): ids to query
        owners (str|list): AWS account ID, "self", "amazon", "aws-marketplace", or "microsoft"
        filters (dict):  EC2 filters, e.g. {"is-public":"true"}
        **kw_filters: filters in create_filters form, e.g. architecture="x86_64"
    Returns:
        list: boto3.resources.factory.ec2.Image objects

    Example:

        >>> get_images(executableusers="self", owners="amazon")

    """
    images = [
        ec2_res.Image(v)
        for v in describe_images(
            executable_users, image_ids, owners, "[*].ImageId", filters, **kw_filters
        )
    ]
    return images
