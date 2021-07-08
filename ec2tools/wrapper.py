"""
User-friendly wrappers to boto3.client("ec2").describe_* functions:
    boto3.client("ec2").describe_tags
    boto3.client("ec2").describe_volumes
    boto3.client("ec2").describe_instances
    boto3.client("ec2").describe_key_pairs
    boto3.client("ec2").describe_security_groups
    boto3.client("ec2").describe_instance_types
    boto3.client("ec2").describe_images
"""

import boto3
from .ec2tools import get

CLIENT_ec2 = boto3.client("ec2")
RESOURCE_ec2 = boto3.resource("ec2")


def _translate_filters(**kw_filters):
    """
    Convert kwargs with underscore-separated keys to dict with hyphen-separated keys
    like EC2 filters.

    EC2 filters are key-values pairs where the key is lower case and hyphen-separated,
    and the values are a list of strings.  This function will
    1. convert underscore-separated Python kwargs to hyphen-separated strings for EC2,
      1b. any key of the form "tag_*" other than "tag_key" will be converted into the special
          filter string "tag:*".  If the key does not start with "tag_" don't worry about this...
    2. if a single string value is given, creates a one-element list containing the string
    3. creates a suitable JSON-like dict for boto3 filters.

    Args:
        **kw_filters: filter key-value pairs.  The keys are Pythonic (underscored) versions
            of EC2 filter keys, and the values should be strings or lists of strings.
    Returns:
        dict: filters with translated keys
    
    Examples:
    
        >>> _translate_filters(resource_type="instance")
        {'resource-type': 'instance'}
        
        >>> _translate_filters(tag_Name="instance")
        {'tag:Name': 'instance'}
    
    EC2 commands each have their own valid filter names!  Check on the command line, e.g.
    
        >>> aws2 ec2 describe-instances help
        
        >>> aws2 ec2 describe-images help
    
    and so on to see lists of valid filter names.
    """
    out = {}
    for key, value in kw_filters.items():
        if value is None:
            raise Exception(
                f"Value for key '{key}' is None, but should be a string or list of strings"
            )

        if key.startswith("tag_") and key != "tag_key":
            key = key.replace("_", ":")
        else:
            key = key.replace("_", "-")
        out[key] = value
    return out


def _create_ec2_filters(filters):
    """
    Quick filter creation for describe-images, describe-instances, etc.

    To create an EC2 Filters object such as
        [{'Name': 'resource-type', 'Values': ['instance']}]
    call _create_ec2_filters with a dict:
        _create_ec2_filters({'resource-type': 'instance'})
    or
        _create_ec2_filters({'resource-type': ['instance']})

    A valid EC2 Filters object is a JSON-like list of individual filters
    such as
        [{'Name': 'tag:Name', 'Values': ['instance']}]
    where each filter is a dict with 'Name' and 'Values' keys, and the
    'Values' must be a list of strings.
    
    Args:
        filters (dict): EC2 filter keys and associated values
    Returns:
        list: JSON-like EC2 Filters object

    Examples:
        >>> _create_ec2_filters({'resource-type': 'instance'})
        [{'Name': 'resource-type', 'Values': ['instance']}]

        >>> _create_ec2_filters({'tag:Name': 'instance'})
        [{'Name': 'tag:Name', 'Values': ['instance']}]

        >>> _create_ec2_filters({'tag:Name': ['instance']})
        [{'Name': 'tag:Name', 'Values': ['instance']}]

        >>> _create_ec2_filters({'is-public': True})
        [{'Name': 'is-public', 'Values': ['true']}]

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

        value = _to_list(value)

        ec2_filters.append({"Name": key, "Values": value})

    return ec2_filters


def _to_list(scalar_or_list):
    """
    Puts scalar arguments into a single-element list, but None
    remains None.  Lists are passed through.

    Args:
        scalar_or_list: None, or a scalar object, or a list
    Returns:
        list|None: scalar args as single element lists; None and list
            will be passed through unchanged.

    Examples:
        >>> _to_list(1)
        [1]

        >>> _to_list(["a", "b"])
        ['a', 'b']

        >>> _to_list(None)  # returns None
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
    
    The filters dict will be passed to _create_ec2_filters.  Check the docstring for _create_ec2_filters.
    
    Args:
        id_field_name (str): something plural like "VolumeIds"
        ids (None|str|list): list of ids (volume ids, instance ids)
        filters (dict): valid filters in EC2 form, e.g. {"egress.ip-permission.cidr":"*"}
        kw_filters (dict): valid filters in _create_ec2_filters form, e.g. {"size":"8"}
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


def describe_tags(path="$[*]", filters=None, **kw_filters):
    """
    Simpler access to describe_tags().  For a list of valid filter names, consult

        >>> aws2 ec2 describe-tags help

    and look at the --filters option.
    
    Args:
        path (str): JSONpath query
        filters (dict): EC2 filters, e.g. {"attachment.delete-on-termination":True}
        kw_filters (dict): valid filters in _create_ec2_filters form, e.g. size="8"
    Returns:
        object: JSON-like
    """
    kwargs = _describe_kwargs("ID_FIELD_NAME_UNUSED", None, filters, kw_filters)
    v = CLIENT_ec2.describe_tags(**kwargs)
    return get(v["Tags"], path)


def describe_volumes(volume_ids=None, path="$[*]", filters=None, **kw_filters):
    """
    Simpler access to describe_volumes().  For a list of valid filter names, consult

        >>> aws2 ec2 describe-volumes help

    and look at the --filters option.
    
    Args:
        volume_ids (str|list): ids to query
        path (str): JSONpath query
        filters (dict): EC2 filters, e.g. {"attachment.delete-on-termination":True}
        kw_filters (dict): valid filters in _create_ec2_filters form, e.g. size="8"
    Returns:
        object: JSON-like
    """
    kwargs = _describe_kwargs("VolumeIds", volume_ids, filters, kw_filters)
    v = CLIENT_ec2.describe_volumes(**kwargs)
    return get(v["Volumes"], path)


def get_volumes(volume_ids=None, filters=None, **kw_filters):
    """
    Get list of volumes.  For a list of valid filter names, consult

        >>> aws2 ec2 describe-volumes help

    and look at the --filters option.
    
    Args:
        volume_ids (str|list): ids to query
        filters (dict): EC2 filters, e.g. {"attachment.delete-on-termination":True}
        kw_filters (dict): filters in _create_ec2_filters form, e.g. size="8"
    Returns:
        list: boto3.resources.factory.ec2.Volume objects

    Example:
        >>> get_volumes(size="8") # filter for size
        >>> get_volumes(volume_ids="vol-0abec2c0447f5bbb8") # get particular volume
        >>> get_volumes(volume_id="vol*") # filter volume IDs with wildcard
    """
    volumes = [
        RESOURCE_ec2.Volume(v)
        for v in describe_volumes(volume_ids, "[*].VolumeId", filters, **kw_filters)
    ]
    return volumes


def describe_instances(instance_ids=None, path="$.[*]", filters=None, **kw_filters):
    """
    Simpler access to describe_instances().  For a list of valid filter names, consult

        >>> aws2 ec2 describe-instances help

    and look at the --filters option.
    
    Args:
        instance_ids (str|list): ids to query
        path (str): JSONpath query
        filters (dict):  EC2 filters, e.g. {"block-device-mapping.device-name":"/dev/xvda"}
        **kw_filters: filters in _create_ec2_filters form, e.g. instance_type="t2.micro"
    Returns:
        object: JSON-like
    
    Examples:
        # Get IDs for instances with name matching the "myInstance*" pattern
        >>> describe_instances(path="[*].Instances[*].InstanceId", tag_Name="myInstance*")

        # Get full descriptions for instances with name equal to "ServerName"
        >>> describe_instances(tag_Name="Server Name")

        # Get only the InstanceId fields, with a more economical query path.
        >>> describe_instances(path="$..InstanceId")
    """
    kwargs = _describe_kwargs("InstanceIds", instance_ids, filters, kw_filters)
    v = CLIENT_ec2.describe_instances(**kwargs)
    return get(v["Reservations"], path)


def get_instances(instance_ids=None, filters=None, **kw_filters):
    """
    Get list of instances.  For a list of valid filter names, consult

        >>> aws2 ec2 describe-instances help

    and look at the --filters option.
    
    Args:
        instance_ids (str|list): ids to query
        filters (dict):  EC2 filters, e.g. {"block-device-mapping.device-name":"/dev/xvda"}
        **kw_filters: filters in _create_ec2_filters form, e.g. instance_type="t2.micro"
    Returns:
        list: boto3.resources.factory.ec2.Instance objects

    Examples:
        # Get instances with name matching the "myInstance*" pattern
        >>> get_instances(tag_Name="myInstance*")

        # Get instances with name equal to "ServerName"
        >>> get_instances(tag_Name="Server Name")

        # Get instances with image_id starting with "ami"
        >>> get_instances(image_id="ami*")
    """
    instances = [
        RESOURCE_ec2.Instance(v)
        for v in describe_instances(
            instance_ids, "[*].Instances[*].InstanceId", filters, **kw_filters
        )
    ]
    return instances


def describe_key_pairs(key_pair_ids=None, path="$.[*]", filters=None, **kw_filters):
    """
    Simpler access to describe_key_pairs().  For a list of valid filter names, consult

        >>> aws2 ec2 describe-key-pairs help

    and look at the --filters option.
    
    Args:
        key_pair_ids (str|list): ids to query
        path (str): JSONpath query
        filters (dict):  EC2 filters, e.g. {"key-name":"admin-key-pair-us-west-1"}
        **kw_filters: filters in _create_ec2_filters form, e.g. key_name="admin-key-pair-us-west-1"
    Returns:
        object: JSON-like

    Examples:
        >>> describe_key_pairs(key_pair_id='key-0123456789abcdef0')

        >>> describe_key_pairs(filters={"key-pair-id":"key-0123456789abcdef0"})
        
        >>> describe_key_pairs(path="[*].KeyName")
    """
    kwargs = _describe_kwargs("KeyPairIds", key_pair_ids, filters, kw_filters)

    v = CLIENT_ec2.describe_key_pairs(**kwargs)
    return get(v["KeyPairs"], path)


def get_key_pairs(key_pair_ids=None, filters=None, **kw_filters):
    """
    Get list of key pairs.  For a list of valid filter names, consult

        >>> aws2 ec2 describe-key-pairs help

    and look at the --filters option.
    
    Args:
        key_pair_ids (str|list): ids to query
        filters (dict):  EC2 filters, e.g. {"key-name":"admin-key-pair-us-west-1"}
        **kw_filters: filters in _create_ec2_filters form, e.g. key_name="admin-key-pair-us-west-1"
    Returns:
        list: boto3.resources.factory.ec2.KeyPair objects

    Examples:
        >>> get_key_pairs(key_pair_id='key-0123456789abcdef0')

        >>> get_key_pairs(filters={"key-pair-id":"key-0123456789abcdef0"})
        
        >>> get_key_pairs(path="[*].KeyName")
    """
    key_pairs = [
        RESOURCE_ec2.KeyPair(v)
        for v in describe_key_pairs(
            key_pair_ids, "[*].KeyPairId", filters, **kw_filters
        )
    ]
    return key_pairs


def describe_security_groups(
    security_group_ids=None, path="$.[*]", filters=None, **kw_filters
):
    """
    Simpler access to describe_security_groups().  For a list of valid filter names, consult

        >>> aws2 ec2 describe-security-groups help

    and look at the --filters option.
    
    Args:
        security_group_ids (str|list): ids to query
        path (str): JSONpath query
        filters (dict):  EC2 filters, e.g. {"ip-permission.from-port":"80"}
        **kw_filters: filters in _create_ec2_filters form, e.g. group_name="admin_SG_us_west1"
    Returns:
        object: JSON-like

    Examples:
        >>> describe_security_groups(group_id="sg-0123456fb1f21afb3")

        >>> describe_security_groups(filters={"group-id":"sg-0123456fb1f21afb3"})

        >>> describe_security_groups(filters={"ip-permission.from-port":"25565"})

        >>> describe_security_groups(path="[*].Description")
    """
    kwargs = _describe_kwargs(
        "SecurityGroupIds", security_group_ids, filters, kw_filters
    )
    v = CLIENT_ec2.describe_security_groups(**kwargs)
    return get(v["SecurityGroups"], path)


def get_security_groups(security_group_ids=None, filters=None, **kw_filters):
    """
    Get list of security groups.  For a list of valid filter names, consult

        >>> aws2 ec2 describe-security-groups help

    and look at the --filters option.
    
    Args:
        security_group_ids (str|list): ids to query
        filters (dict):  EC2 filters, e.g. {"ip-permission.from-port":"80"}
        **kw_filters: filters in _create_ec2_filters form, e.g. group_name="admin_SG_us_west1"
    Returns:
        list: boto3.resources.factory.ec2.SecurityGroup objects

    Examples:
        >>> get_security_groups(group_id="sg-0123456fb1f21afb3")

        >>> get_security_groups(filters={"group-id":"sg-0123456fb1f21afb3"})

        >>> get_security_groups(filters={"ip-permission.from-port":"25565"})
    """
    security_groups = [
        RESOURCE_ec2.SecurityGroup(v)
        for v in describe_security_groups(
            security_group_ids, "[*].GroupId", filters, **kw_filters
        )
    ]
    return security_groups


def describe_instance_types(
    instance_types=None, path="$.[*]", filters=None, **kw_filters
):
    """
    Simpler access to describe_instance_types().  For a list of valid filter names, consult

        >>> aws2 ec2 describe-instance-types help

    and look at the --filters option.
    
    Args:
        instance_types (str|list): ids to query
        path (str): JSONpath query
        filters (dict):  EC2 filters, e.g. {"ip-permission.from-port":"80"}
        **kw_filters: filters in _create_ec2_filters form, e.g. group_name="admin_SG_us_west1"
    Returns:
        object: JSON-like

    Examples:
        >>> describe_instance_types("m5.large")

        >>> describe_instance_types(free_tier_eligible=True)

        >>> describe_instance_types(filters={"vcpu-info.default-vcpus":"1"})
    """
    kwargs = _describe_kwargs("InstanceTypes", instance_types, filters, kw_filters)
    v = CLIENT_ec2.describe_instance_types(**kwargs)
    return get(v["InstanceTypes"], path)


# ****** There is no get_instance_types because there is no InstanceType class. ******
def get_instance_types():
    """
    get_instance_types is undefined because there is no InstanceType class.
    """
    raise Exception(
        "get_instance_types is undefined because there is no InstanceType class."
    )


def describe_images(
    executable_users=None,
    image_ids=None,
    owners=None,
    path="$.[*]",
    filters=None,
    **kw_filters,
):
    """
    Simpler access to describe_images().  For a list of valid filter names, consult

        >>> aws2 ec2 describe-images help

    and look at the --filters option.
    
    Args:
        executable_users (str|list): AWS account ID, "self", or "all" (public AMIs)
        image_ids (str|list): ids to query
        owners (str|list): AWS account ID, "self", "amazon", "aws-marketplace", or "microsoft"
        path (str): JSONpath query
        filters (dict):  EC2 filters, e.g. {"is-public":"true"}
        **kw_filters: filters in _create_ec2_filters form, e.g. architecture="x86_64"
    Returns:
        object: JSON-like
    """
    kwargs = _describe_kwargs("ImageIds", image_ids, filters, kw_filters)

    if executable_users is not None:
        kwargs.update({"ExecutableUsers": _to_list(executable_users)})
    if owners is not None:
        kwargs.update({"Owners": _to_list(owners)})

    v = CLIENT_ec2.describe_images(**kwargs)
    return get(v["Images"], path)


def get_images(
    executable_users=None, image_ids=None, owners=None, filters=None, **kw_filters
):
    """
    Get list of images.  For a list of valid filter names, consult

        >>> aws2 ec2 describe-images help

    and look at the --filters option.
    
    Args:
        executable_users (str|list): AWS account ID, "self", or "all" (public AMIs)
        image_ids (str|list): ids to query
        owners (str|list): AWS account ID, "self", "amazon", "aws-marketplace", or "microsoft"
        filters (dict):  EC2 filters, e.g. {"is-public":"true"}
        **kw_filters: filters in _create_ec2_filters form, e.g. architecture="x86_64"
    Returns:
        list: boto3.resources.factory.ec2.Image objects

    Examples:

        >>> get_images(executableusers="self", owners="amazon")

    """
    images = [
        RESOURCE_ec2.Image(v)
        for v in describe_images(
            executable_users, image_ids, owners, "[*].ImageId", filters, **kw_filters
        )
    ]
    return images
