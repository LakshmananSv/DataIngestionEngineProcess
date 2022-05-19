import json
import urllib.parse
import boto3
import datetime

print('Loading function')

s3 = boto3.client('s3')

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        print("CONTENT TYPE: " + response['ContentType'])
    
        client = boto3.client('ec2')
        
        date = datetime.datetime.utcnow().strftime('%Y%M%D-%H_%M_%S')
        imgName = "InstanceID_"+'10.25.128.200'+"_Image_Backup_"+date
        
        targetAsgName = 'yourAutoscalingGroupName'
        autoScalingClient = boto3.client('autoscaling')
        
        CreateImageResponse = client.create_image(InstanceId=getSourceInstanceId(autoScalingClient,targetAsgName), Name=imgName)
        image_Id = CreateImageResponse['ImageId']
        print(image_Id)
        newLaunchConfigName = 'LC '+ image_Id + ' ' + date

        

        CreateLaunchConfiguration(autoScalingClient,getSourceInstanceId(autoScalingClient,targetAsgName),targetAsgName,newLaunchConfigName, image_Id)
    
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

def getSourceInstanceId(autoScalingClient, targetAsgName):
    autoScalingResponse =  autoScalingClient.describe_auto_scaling_groups(AutoScalingGroupNames=[targetAsgName])  
    if not autoScalingResponse['AutoScalingGroups']:
        return 'No such ASG'  
    return autoScalingResponse.get('AutoScalingGroups')[0]['Instances'][0]['InstanceId']

def CreateLaunchConfiguration(autoScalingClient, sourceInstanceId, targetAsgName, newLaunchConfigName, image_Id):
            autoScalingClient.create_launch_configuration(InstanceId = sourceInstanceId,
                                                          LaunchConfigurationName=newLaunchConfigName,
                                                          ImageId= image_Id)
            response = autoScalingClient.update_auto_scaling_group(AutoScalingGroupName = targetAsgName ,LaunchConfigurationName = newLaunchConfigName)
    
            return 'Updated ASG  with new launch configuration `%s` which includes AMI `%s`.' % ( newLaunchConfigName, image_Id)