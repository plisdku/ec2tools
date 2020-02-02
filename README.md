# EC2 tools
## Version 0.0.1

Paul Hansen 
February 1, 2020

## Summary

Convenience functions to handle AWS instances from inside Python.

## Example:

```
import ec2tools

# Get all instances (uses boto3)
instances = ec2tools.get_instances()

# Add or update entries in your ssh config file for your instances.
ec2tools.update_ssh_config("~/.ssh/config", instances, "~/.ssh/amazon_aws")

# Change instance state.  Print a nice waiting message until desired state is reached.

instances[0].stop()
ec2tools.wait_for_state(instances[0], "stopped")

instances[1].start()
ec2tools.wait_for_state(instances[0], "running")
```


