from aws_cdk import (
    Stack, Duration,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_sqs as sqs,
    aws_events as events,
    aws_events_targets as targets,
    aws_lambda_event_sources as es,
    aws_s3_notifications as s3n,
)
from constructs import Construct

class RearcQuestStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # 1. Bucket
        bucket = s3.Bucket(self, "DataBucket", versioned=True)

        # 2. Daily ETL Lambda
        etl_fn = _lambda.Function(
            self, "EtlLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="index.handler",
            code=_lambda.Code.from_asset("src/data_fetch"),
            environment={"BUCKET": bucket.bucket_name},
            timeout=Duration.minutes(5),
        )
        bucket.grant_read_write(etl_fn)

        # 3. Daily schedule
        events.Rule(
            self, "DailyRule",
            schedule=events.Schedule.rate(Duration.days(1)),
            targets=[targets.LambdaFunction(etl_fn)],
        )

        # 4. SQS queue
        queue = sqs.Queue(self, "PopQueue", visibility_timeout=Duration.minutes(5))

        # 5. Analytics Lambda
        analytics_fn = _lambda.Function(
            self, "AnalyticsLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="index.handler",
            code=_lambda.Code.from_asset("src/data_analysis"),
            environment={"BUCKET": bucket.bucket_name},
            timeout=Duration.minutes(5),
        )
        bucket.grant_read(analytics_fn)
        queue.grant_consume_messages(analytics_fn)

        # 6. Hook SQS to Lambda
        analytics_fn.add_event_source(es.SqsEventSource(queue, batch_size=1))

        # 7. S3 -> SQS on new population JSON
        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(queue),
            s3.NotificationKeyFilter(prefix="datausa/", suffix=".json"),
        )