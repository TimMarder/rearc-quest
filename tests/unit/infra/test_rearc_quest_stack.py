import aws_cdk as cdk
from aws_cdk import assertions
from src.infra.rearc_quest_stack import RearcQuestStack

def synth_template():
    app = cdk.App()
    stack = RearcQuestStack(app, "test-stack")
    return assertions.Template.from_stack(stack)

def test_bucket_created():
    template = synth_template()
    template.resource_count_is("AWS::S3::Bucket", 1)

def test_lambda_functions_and_handlers():
    template = synth_template()
    template.resource_count_is("AWS::Lambda::Function", 3) # 3 because of the auto-generated Bucket Notification Lambda

def test_eventbridge_rule():
    template = synth_template()
    template.has_resource_properties(
        "AWS::Events::Rule",
        {"ScheduleExpression": "rate(1 day)"}
    )

def test_sqs_and_subscription():
    template = synth_template()
    template.resource_count_is("AWS::SQS::Queue", 1)
    template.resource_count_is("AWS::Lambda::EventSourceMapping", 1)

def test_s3_to_sqs_notification_custom_resource():
    template = synth_template()
    template.resource_count_is("Custom::S3BucketNotifications", 1)