import boto3
import boto3.ec2
from jsonpath_rw import jsonpath, parse
import sshconf
import os
import re

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
    instance_attributes = dict([(key,instance.__getattribute__(key)) for key in INSTANCE_ATTRIBUTES])
    return instance_attributes

def get_instance_ids(name=None):
    """
    Get a list of instance IDs, optionally matching a pattern.
    
    Args:
        name (str, optional): regexp pattern for instance name
    Returns:
        list: matching instance IDs
    """
    ec2 = boto3.client("ec2")
    
    if name:
        tags_dict = ec2.describe_tags(
            Filters=[{"Name":"value", "Values":[name]}],
        )
        instance_ids = get(tags_dict, "Tags.[*].ResourceId")
    else:
        instances_dict = ec2.describe_instances()
        instance_ids = get(instances_dict, "Reservations[*].Instances[*].InstanceId")
    return instance_ids

def get_instances(name=None):
    """
    Get a list of instances, optionally matching a pattern.
    
    Args:
        name (str, optional): regexp pattern for instance name
    Returns:
        list: matching instances
    """
    ec2_resource = boto3.resource("ec2")
    
    instance_ids = get_instance_ids(name=name)
    
    instances = [ec2_resource.Instance(instance_id) for instance_id in instance_ids]
    return instances



USERNAME_DICT = {"^Amazon Linux.*": "ec2-user"}

def get_instance_username(image_description):
    """
    Get Linux username for given AMI type.
    
    Args:
        image_description (str): 
    """
    if not isinstance(image_description, str):
        raise TypeError(f"image_description ({image_description}) should be str)")
    
    for (pattern, username) in USERNAME_DICT.items():
        result = re.match(pattern, image_description)
        if result:
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
    username = get_instance_username(instance.image.description)
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
        c.add(items["Host"], Hostname=items["Hostname"], User=items["User"], IdentityFile=items["IdentityFile"])
    
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
            msg_len = max(len(msg), msg_len) # need for stupid line rewrite at end
            sys.stdout.write(msg)
        sleep_count += 1
        time.sleep(1)
        
        for instance in instances:
            instance.reload()
        
        states = [instance.state["Name"] for instance in instances]
        
        if sleep_count > timeout:
            raise Exception(f"Timeout occurred (t > {timeout} s).")
    
    if verbose:
        sys.stdout.write("\r" + " "*msg_len)
        sys.stdout.write("\rDone.")
