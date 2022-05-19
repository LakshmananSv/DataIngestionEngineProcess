#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ======================================================
# Created By  : Lakshmanan Sivasubramanian             #
# Created Date: Sun May 15 2022                       #
# ======================================================
"""The Module has been built for creating EC2 Cloudformation Template"""
# import modules
from troposphere import GetAtt, Output, Ref, Template, Tags, Join
from troposphere.ec2 import Instance
from troposphere.elasticloadbalancing import LoadBalancer, Listener, HealthCheck, ConnectionSettings
from troposphere.autoscaling import AutoScalingGroup, LaunchConfiguration
from troposphere.policies import CreationPolicy, UpdatePolicy, AutoScalingRollingUpdate, ResourceSignal
import base64
import json
import yaml
import sys

# loading configuration values to the troposphere script
def load_config(file, flattern=True):
    if file:
        with open(file, "r") as stream:
            try:
                userconfig=yaml.load(stream, Loader=yaml.SafeLoader)
                if flattern:
                    return userconfig["config"]
                else:
                    return userconfig
            except yaml.YAMLError as e:
                pass
            try:
                userconfig=json.load(stream)
                if flattern:
                    return userconfig["config"]
                else:
                    return userconfig
            except Exception as e:
                pass

# constructing EC2 and ELB cloudformation template
def define_cft():
    config = load_config(sys.argv[1])
    z = Template()
    z.add_description("Creating EC2 Template")
    # defining creation policy for autoscaling group to provision EC2 instances
    create_policy=CreationPolicy(
        'EC2CreationPolicy',
        ResourceSignal=ResourceSignal(
            Count=config['CFT']['asg']['createpolicy']['count'],
            Timeout=config['CFT']['asg']['createpolicy']['timeout'],
        )
    )

    # defining update policy for autoscaling group to perform roll stack update of EC2 instances
    update_policy=UpdatePolicy(
        'EC2UpdationPolicy',
        AutoScalingRollingUpdate=AutoScalingRollingUpdate(
            'EC2RollingUpdates',
            MaxBatchSize=config['CFT']['asg']['updatepolicy']['maxbatchsize'],
            WaitOnResourceSignals=config['CFT']['asg']['updatepolicy']['waitonresourcesignal'],
            MinInstancesInService=config['CFT']['asg']['updatepolicy']['mininstances'],
            MinSuccessfulInstancesPercent=config['CFT']['asg']['updatepolicy']['minsucinstpercent'],
            PauseTime=config['CFT']['asg']['updatepolicy']['pausetime'],
        )
    )

    # defining launch configuration to launch EC2 instance with custom settings
    z.add_resource(LaunchConfiguration(
        config['CFT']['lc']['name'],
        InstanceType=config['CFT']['lc']['type'],
        ImageId=config['CFT']['lc']['ami'],
        IamInstanceProfile=config['CFT']['lc']['instance_role'],
        SecurityGroups=config['CFT']['lc']['sgs']
    ))

    # defining autoscaling to scale instances when there is any failure.
    z.add_resource(AutoScalingGroup(
        config['CFT']['asg']['name'],
        DesiredCapacity=config['CFT']['asg']['capacity']['desired'],
        MaxSize=config['CFT']['asg']['capacity']['max'],
        MinSize=config['CFT']['asg']['capacity']['min'],
        LaunchConfigurationName=Ref(config['CFT']['lc']['name']),
        Cooldown=300,
        VPCZoneIdentifier=config['CFT']['asg']['subnets'],
        Tags=config['CFT']['tags']['asg'],
        UpdatePolicy=update_policy,
        CreationPolicy=create_policy
    ))
    return(z.to_json())

if __name__ == '__main__':
    output=define_cft()
    with open('EC2Template.json', 'w') as outfile:
        outfile.write(output)