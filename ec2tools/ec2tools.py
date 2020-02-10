import boto3
import boto3.ec2
from jsonpath_rw_ext import parse
import sshconf
import os
import re
import sys
import time


def get(obj, expr):
    """
    Quick jsonpath query.
    
    Args:
        obj (object): JSON object e.g. dict or list
        expr (str): JSONpath expression
    Returns:
        list: matches
    """
    jsonpath_expr = parse(expr)
    return [match.value for match in jsonpath_expr.find(obj)]


INSTANCE_ATTRIBUTES = [
    "ami_launch_index",
    "architecture",
    "block_device_mappings",
    "capacity_reservation_id",
    "capacity_reservation_specification",
    "client_token",
    "cpu_options",
    "ebs_optimized",
    "elastic_gpu_associations",
    "elastic_inference_accelerator_associations",
    "ena_support",
    "hibernation_options",
    "hypervisor",
    "iam_instance_profile",
    "image_id",
    "instance_id",
    "instance_lifecycle",
    "instance_type",
    "kernel_id",
    "key_name",
    "launch_time",
    "licenses",
    "metadata_options",
    "monitoring",
    "network_interfaces_attribute",
    "outpost_arn",
    "placement",
    "platform",
    "private_dns_name",
    "private_ip_address",
    "product_codes",
    "public_dns_name",
    "public_ip_address",
    "ramdisk_id",
    "root_device_name",
    "root_device_type",
    "security_groups",
    "source_dest_check",
    "spot_instance_request_id",
    "sriov_net_support",
    "state",
    "state_reason",
    "state_transition_reason",
    "subnet_id",
    "tags",
    "virtualization_type",
    "vpc_id",
]


def get_instance_attributes(instance):
    """
    Get attributes of instance as a dict.
    
    The attributes extracted are the full list from the AWS documentation on Feb 1, 2020.
    
    Args:
        instance (boto3.resources.factory.ec2.Instance): instance to query
    Returns:
        dict: attributes
    """
    instance_attributes = dict(
        [(key, getattr(instance, key, None)) for key in INSTANCE_ATTRIBUTES]
    )
    return instance_attributes


def create_tag_spec(resource_type="instance", **kwargs):
    """
    Make a tag spec for EC2 stuff.
    """
    tags = []
    for (key, value) in kwargs.items():
        tags.append({"Key": key, "Value": value})

    tag_spec = [{"ResourceType": "instance", "Tags": tags}]
    return tag_spec


def get_instance_ids(name=None):
    """
    Get a list of instance IDs, optionally matching a pattern.
    
    Args:
        name (str, optional): regexp pattern for instance name, e.g. "plisdku.*"
    Returns:
        list: matching instance IDs
    """
    ec2 = boto3.client("ec2")

    if name:
        filters = [
            {"Name": "value", "Values": [name]},
            {"Name": "resource-type", "Values": ["instance"]},
        ]
        tags_dict = ec2.describe_tags(Filters=filters)
        instance_ids = get(tags_dict, "Tags.[*].ResourceId")
    else:
        instances_dict = ec2.describe_instances()
        instance_ids = get(instances_dict, "Reservations[*].Instances[*].InstanceId")
    return instance_ids


def get_instances(name=None, instance_id=None):
    """
    Get a list of instances, optionally matching a pattern.
    
    Args:
        name (str, optional): regexp pattern for instance name
    Returns:
        list: matching instances
    """
    ec2_resource = boto3.resource("ec2")

    instances = []

    if name is not None:
        instance_ids = get_instance_ids(name=name)
        instances = [ec2_resource.Instance(instance_id) for instance_id in instance_ids]
    elif instance_id is not None:
        instance_ids = get_instance_ids()
        if instance_id not in instance_ids:
            raise Exception(f"Instance id {instance_id} is not a valid instance.")
        instances = [ec2_resource.Instance(instance_id)]
    else:
        instance_ids = get_instance_ids()
        instances = [ec2_resource.Instance(instance_id) for instance_id in instance_ids]

    return instances


USERNAME_DICT = {".*Amazon Linux.*": "ec2-user"}


def get_image_username(image):
    """
    Get Linux username for given AMI type.
    
    Args:
        image (boto3.resources.factory.ec2.Image): image to query
    Returns:
        str: username
    """

    # 1. Try to match image name
    # 2. Try to match image description
    for (pattern, username) in USERNAME_DICT.items():
        if re.match(pattern, image.name):
            return username
        if re.match(pattern, image.description):
            return username
    return None


def get_instance_tags(instance, key):
    """
    Get values for all instance tags with the given key.
    
    Paul is uncertain whether EC2 instances may have duplicate tag keys.
    
    Args:
        instance (boto3.resources.factory.ec2.Instance): instance to query
        key (str): tag name
    Returns:
        list: values of all tags named key
    """
    tags = []
    for tag in instance.tags:
        if tag["Key"] == key:
            tags.append(tag["Value"])
    return tags


def get_instance_name(instance):
    """
    Get name of instance.  Returns empty string if no name is given.
    
    Args:
        instance (boto3.resources.factory.ec2.Instance): instance to query
    Returns:
        str
    """
    name_tags = get_instance_tags(instance, "Name")
    if len(name_tags) == 1:
        return name_tags[0]
    else:
        return ""


def get_instance_ssh_config_items(instance, pem_dir_path):
    """
    Create dict of fields to put in SSH config file.
    
    Args:
        instance (boto3.resources.factory.ec2.Instance): instance to insert in config
        pem_dir_path (str): path to where PEM file should be stored
    Returns:
        dict: Host, Hostname, User and IdentityFile
    """
    username = get_image_username(instance.image)
    name = get_instance_name(instance)

    identity_file = os.path.join(pem_dir_path, f"{instance.key_name}.pem")

    # I can't have spaces in the host name.
    if name == "" or " " in name:
        host = instance.public_ip_address
    else:
        host = name

    out_dict = {
        "Host": host,
        "Hostname": instance.public_dns_name,
        "User": username,
        "IdentityFile": identity_file,
    }

    return out_dict


def get_quota_codes():
    """
    Get all EC2 quota codes.
    
    Returns:
        dict: QuotaName -> QuotaCode
    """
    quotas = boto3.client("service-quotas")
    result = quotas.list_service_quotas(ServiceCode="ec2")
    quota_names = get(result, "Quotas[*].QuotaName")
    quota_codes = get(result, "Quotas[*].QuotaCode")
    quota_code_dict = dict(zip(quota_names, quota_codes))
    return quota_code_dict


def get_quota(quota_name=None, quota_code=None, quota_field=None):
    """
    Get quota dict for a given EC2 service.
    
    Example of EC2 service: "Running On-Demand G instances".
    Its quota code is "L-DB2E81BA".
    
    For full list of EC2 service quota codes, call get_quota_codes().
    
    Args:
        quota_name (str): EC2 service name such as "Running On-Demand G instances"
        quota_code (str): EC2 service quota code such as "L-DB2E81BA"
        quota_field (str): Single field of quota dict such as "Value"
    Returns:
        service quota dict, or value of its quota_field field
    """
    if quota_name is not None:
        if quota_code:
            raise Exception("Only one of quota_name and quota_code should be provided.")
        try:
            quota_code = get_quota_codes()[quota_name]
        except KeyError as exc:
            msg = f"Unknown quota name '{quota_name}'.\n"
            msg += "Valid quota names:\n\t" + "\n\t".join(get_quota_codes().keys())
            raise Exception(msg) from None
    elif quota_code is None:
        raise Exception("Either quota_name or quota_code should be provided.")

    quotas = boto3.client("service-quotas")
    result = quotas.get_service_quota(ServiceCode="ec2", QuotaCode=quota_code)

    try:
        result = result["Quota"]
    except KeyError as exc:
        raise Exception("Service quota dict has no Quota key!")

    if quota_field is not None:
        try:
            result = result[quota_field]
        except KeyError as exc:
            msg = f"Unknown quota field '{quota_field}'. "
            msg += "Valid fields are " + ", ".join(result.keys()) + "."
            raise Exception(msg) from None

    return result


def get_instance_types(field=None):
    """
    Get list of available EC2 instance types.
    
    Args:
        field (str): field to return e.g. "InstanceType"
    Returns:
        dict
    """
    ec2 = boto3.client("ec2")
    result = ec2.describe_instance_types()["InstanceTypes"]

    if field is not None:
        result = get(result, "[*].InstanceType")

    return result


def get_instance_type_quota(instance_type, quota_field=None):
    """
    Get on-demand instance quota for given instance type.

    Args:
        instance_type (str): EC2 instance type,  e.g. "t1.micro"
    """
    ec2 = boto3.client("ec2")

    if instance_type[0] == "f":
        # there may not be any FPGA instances in my availability zone...?
        quota_name = "Running On-Demand F instances"
    elif instance_type[0] == "g":
        quota_name = "Running On-Demand G instances"
    elif instance_type[0:3] == "inf":
        quota_name = "Running On-Demand Inf instances"
    elif instance_type[0] == "p":
        quota_name = "Running On-Demand P instances"
    elif instance_type[0] == "x":
        quota_name = "Running On-Demand X instances"
    else:
        quota_name = "Running On-Demand Standard (A, C, D, H, I, M, R, T, Z) instances"

    result = get_quota(quota_name, quota_field=quota_field)
    return result


def get_key_pairs(field=None):
    """
    Get key pairs.
    
    Args:
        field (str,optional): name of one field to read e.g. "KeyName"
    Returns:
        json object
    """
    ec2 = boto3.client("ec2")
    result = ec2.describe_key_pairs()["KeyPairs"]
    if field is not None:
        result = get(result, f"[*].{field}")
    return result


def get_security_groups(field=None):
    """
    Get security groups.
    
    Args:
        field (str, optional): name of single field to return e.g. "GroupName", "GroupId"
    Returns:
        json object
    """
    ec2 = boto3.client("ec2")
    result = ec2.describe_security_groups()["SecurityGroups"]
    if field is not None:
        result = get(result, f"[*].{field}")
    return result


def launch_instance(
    image_id,
    instance_type,
    key_name,
    security_group=None,
    instance_name=None,
    dry_run=False,
):
    """
    Launch a new instance.
    
    Args:
        image_id (str): AMI image id e.g. "ami-03caa3f860895f82e"
        instance_type (str): instance type e.g. "t1.micro"
        key_name (str): name of your key pair e.g. "key_sg_uswest1"
        security_group (str, optional): name of security group e.g. "temp_SG_uswest1"
        instance_name (str, optional): name of instance (alias for SSH) e.g. "my.t1.micro"
    Returns:
        boto3.resources.factory.ec2.Instance: the new instance
    """

    kwargs = {
        "ImageId": image_id,
        "InstanceType": instance_type,
        "KeyName": key_name,
        "MinCount": 1,
        "MaxCount": 1,
        "DryRun": dry_run,
    }
    if security_group is not None:
        kwargs["SecurityGroups"] = [security_group]
    if instance_name is not None:
        tags = [{"Key": "Name", "Value": instance_name}]
        tag_spec = [{"ResourceType": "instance", "Tags": tags}]
        kwargs["TagSpecifications"] = tag_spec

    ec2 = boto3.client("ec2")
    reservations = ec2.run_instances(**kwargs)
    instance_ids = get(reservations, "Instances[*].InstanceId")

    ec2res = boto3.resource("ec2")
    instance = ec2res.Instance(instance_ids[0])

    return instance


def update_ssh_config(config_path, instances, pem_dir_path, new_config_path=None):
    """
    Update SSH config file to include the given AWS EC2 instances.
    
    Entries will be added (updated) in the SSH config file.  Example:
    
        Host host_alias
          HostName my_aws_host.compute.amazonaws.com
          User ec2-user
          IdentityFile /Users/paul/.ssh/amazon_aws/plisdku-admin-aws-key-pair-us-west-1.pem
    
    Args:
        config_path (str): path to SSH config file, e.g. ~/.ssh/config
        instances (list): list of boto3.resources.factory.ec2.Instance references
        new_config_path (str, optional): path for writing config file
    """
    config_path = os.path.expanduser(config_path)

    if new_config_path is None:
        new_config_path = config_path
    else:
        new_config_path = os.path.expanduser(config_path)

    try:
        c = sshconf.read_ssh_config(config_path)
    except FileNotFoundError:
        raise

    for instance in instances:
        items = get_instance_ssh_config_items(instance, pem_dir_path)
        if items["Host"] in c.hosts():
            c.remove(items["Host"])

        if instance.state["Name"] in ["running", "pending"]:
            c.add(
                items["Host"],
                Hostname=items["Hostname"],
                User=items["User"],
                IdentityFile=items["IdentityFile"],
            )

    c.write(new_config_path)


def wait_for_state(instance, desired_state, timeout=300, verbose=True):
    """
    Wait until one or several instances are in the desired state.
    
    Probably the only useful states are "running" and "stopping".
    
    Args:
        instance (ec2.Instance or list): instance(s) to wait for
        desired_state (str): state to wait for, e.g. "running" or "stopping"
        timeout (int): duration in seconds (300 by default)
        verbose (bool): whether to print an update message
    Raises:
        Exception: if timeout occurred
    """
    if isinstance(instance, list):
        instances = instance
    else:
        instances = [instance]

    sleep_count = 0

    msg_len = 0

    states = [instance.state["Name"] for instance in instances]
    while any([s != desired_state for s in states]):
        if verbose:
            msg = "\r" + ", ".join(states) + f" [{sleep_count} s]"
            msg_len = max(len(msg), msg_len)  # need for stupid line rewrite at end
            sys.stdout.write(msg)
        sleep_count += 1
        time.sleep(1)

        for instance in instances:
            instance.reload()

        states = [instance.state["Name"] for instance in instances]

        if sleep_count > timeout:
            raise Exception(f"Timeout occurred (t > {timeout} s).")

    if verbose:
        sys.stdout.write("\r" + " " * msg_len)
        sys.stdout.write("\rDone.")
