import sys
import argparse

import ec2tools

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


def init_argparse():
    parser = argparse.ArgumentParser(description="Perform basic EC2 operations.")
    subparsers = parser.add_subparsers(
        title="subcommands", description="valid subcommands", help="sub-command help"
    )

    # ==== Parents

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

    # List instances
    parser_list = subparsers.add_parser("list", help="print help")
    parser_list.add_argument(
        "pattern", nargs="?", type=str, help="instance name pattern"
    )
    parser_list.set_defaults(func=do_list)

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
    instances = ec2tools.get_instances(name=args.pattern)
    print_instances(instances)


def print_instances(instances):
    line_fmt = (
        lambda row, instance_id, name, image_id, image_name, state: f"{row:<3} {instance_id:<22}  {name:<20}  {image_id:<22}  {image_name[:30]:<30}  {state:<10}"
    )

    header = line_fmt("", "ID", "Name", "Image ID", "Image Name", "State")
    print(header)
    print("-" * len(header))

    for row, instance in enumerate(instances):
        name = ec2tools.get(instance.tags, "$[?(@.Key == 'Name')].Value")
        if name:
            name = name[0]
        instance_id = instance.id
        image_name = instance.image.name
        image_id = instance.image.id
        state = instance.state["Name"]

        print(line_fmt(row, instance_id, name, image_id, image_name, state))


def do_start(args):
    instances = _get_instances(args)

    for instance in instances:
        instance.start()
    if args.wait:
        ec2tools.wait_for_state(instances, "running")


def do_stop(args):
    instances = _get_instances(args)

    for instance in instances:
        instance.stop()
    if args.wait:
        ec2tools.wait_for_state(instances, "stopped")


def do_terminate(args):
    instances = _get_instances(args)

    s = "s" if len(instances) else ""

    if len(instances) > 0:
        print("Really terminate {len(instances)} instance{s}?  (hit y to confirm)")
        yes_no = input()
        if yes_no.lower() == "y":
            for instance in instances:
                instance.terminate()


def _get_instances(args):
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
    parser = init_argparse()
    args = parser.parse_args(
        sys.argv[1:]
    )  # explicitly pass argv for setuptools entry-points

    if "func" in args:
        args.func(args)
