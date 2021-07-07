import sys
import argparse

from . import ec2tools

# ec2 list
# ec2 list [pattern]
#
# ec2 start [pattern|id]
# ec2 start --wait [pattern|id] --update-ssh-config
# ec2 start --interactive
#
# ec2 stop --update-ssh-config
# ec2 stop --interactive
#
# ec2 terminate <-- ask for confirmation
# ec2 terminate --interactive --update-ssh-config
#
# ec2 launch [image ID] [instance type] [key name] [security group] [instance name]
# ec2 launch --interactive --update-ssh-config
#
# need a yaml file with defaults, somewhere
#
# ec2 config-template
#
#

# "One particularly effective way of handling sub-commands is to
# combine the use of the add_subparsers() method with calls to
# set_defaults() so that each subparser knows which Python function
# it should execute." -- argparse documentation
#
# So we will create a top-level parser with a list of subparsers.
# Each subparser handles a particular command, such as "ec2 list" or
# "ec2 stop".  I will make a handler for "list" and for "stop".
#
# Because many of the commands will have the same options, like
# an "instance" argument, I can make command "parents" for --instance
# and --wait and so on.  They will donate the --instance and --wait
# arguments to their child commands.


def init_argparse():
    parser = argparse.ArgumentParser(description="Perform basic EC2 operations.")
    subparsers = parser.add_subparsers(
        title="subcommands", description="valid subcommands", help="sub-command help"
    )

    # ==== Parents
    # A command with the instance_arg parent created here will
    # have an --instance option.  A command with the parent
    # wait_arg created here will have a --wait option.

    # Instance command parent
    instance_arg = argparse.ArgumentParser(add_help=False)
    instance_arg.add_argument(
        "instance", nargs="?", type=str, help="instance ID or name"
    )

    # Wait command parent
    wait_arg = argparse.ArgumentParser(add_help=False)
    wait_arg.add_argument(
        "--wait",
        action="store_true",
        help="wait to return to shell until state has changed",
    )

    # ==== Commands
    # These are the actual subcommands for ec2 like "list" and "stop".
    # I make a subparser for each one of these, and they will dispatch
    # to a given function like my do_list() or do_start() to actually
    # deal with the arguments and do the job required.

    # List instances
    parser_list = subparsers.add_parser("list", help="print help")
    parser_list.add_argument(
        "pattern", nargs="?", type=str, help="instance name pattern"
    )
    parser_list.set_defaults(func=do_list)  # when func is provided, *I* call it, below.

    # Start instances
    parser_start = subparsers.add_parser(
        "start", parents=[instance_arg, wait_arg], help="start instance by ID or name"
    )
    parser_start.set_defaults(func=do_start)

    # Stop instances
    parser_start = subparsers.add_parser(
        "stop", parents=[instance_arg, wait_arg], help="stop instance by ID or name"
    )
    parser_start.set_defaults(func=do_stop)

    return parser


def do_list(args):
    """
    Handler for "ec2 list".
    """
    instances = ec2tools.get_instances(name=args.pattern)
    for ii in instances:
        print("Type:", type(ii))
    print_instances(instances)


def print_instances(instances):
    def _line_fmt(row, instance_id, name, image_id, image_name, state):
        return f"{row:<3} {instance_id:<22}  {name:<20}  {image_id:<22}  {image_name[:30]:<30}  {state:<10}"

    # line_fmt = (
    #     lambda row, instance_id, name, image_id, image_name, state: f"{row:<3} {instance_id:<22}  {name:<20}  {image_id:<22}  {image_name[:30]:<30}  {state:<10}"
    # )

    header = _line_fmt("", "ID", "Name", "Image ID", "Image Name", "State")
    print(header)
    print("-" * len(header))

    for row, instance in enumerate(instances):
        name = "(unnamed)"
        if instance.tags is not None:
            name = ec2tools.get(instance.tags, "$[?(@.Key == 'Name')].Value")
            if name:
                name = name[0]
        instance_id = instance.id
        image_name = instance.image.name
        image_id = instance.image.id
        state = instance.state["Name"]

        print(_line_fmt(row, instance_id, name, image_id, image_name, state))


def do_start(args):
    """
    Handler for "ec2 start".
    """
    instances = _get_instances(args)

    for instance in instances:
        instance.start()
    if args.wait:
        ec2tools.wait_for_state(instances, "running")


def do_stop(args):
    """
    Handler for "ec2 stop".
    """
    instances = _get_instances(args)

    for instance in instances:
        instance.stop()
    if args.wait:
        ec2tools.wait_for_state(instances, "stopped")


def do_terminate(args):
    """
    Handler for "ec2 terminate".
    """
    instances = _get_instances(args)

    s = "s" if len(instances) else ""

    if len(instances) > 0:
        print("Really terminate {len(instances)} instance{s}?  (hit y to confirm)")
        yes_no = input()
        if yes_no.lower() == "y":
            for instance in instances:
                instance.terminate()


def _get_instances(args):
    """
    Get list of boto3.

    Args:
        args (dict): key-value pairs.  args["pattern"] may be an instance ID or a regexp pattern

    Returns:
        list: boto3.resources.factory.ec2.Instance objects
    """
    instances = []
    if "pattern" in args:
        instances = ec2tools.get_instances(name=args.pattern)
        if len(instances) == 0:
            instances = ec2tools.get_instances(instance_id=args.pattern)
    else:
        all_instances = ec2tools.get_instances()
        if len(all_instances) == 0:
            instances = []
        else:
            print_instances(all_instances)
            print(f"Select instance number from list (0 to {len(all_instances)-1}):")
            idx = input()

            try:
                idx = int(idx)
                if idx > 0 and idx < len(all_instances):
                    instances = all_instances[idx : idx + 1]
            except ValueError as exc:
                print(f"Index should be a number from 0 to {len(all_instances)-1}")
    return instances


def main():
    """
    Main function.  Determines which subcommand is given, such
    as ec2 list or ec2 stop, then handles the command, then exits.
    """
    parser = init_argparse()
    args = parser.parse_args(
        sys.argv[1:]
    )  # explicitly pass argv for setuptools entry-points

    if "func" in args:
        args.func(args)
