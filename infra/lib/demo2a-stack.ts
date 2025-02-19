import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as path from 'path';
import * as s3deploy from 'aws-cdk-lib/aws-s3-deployment';

export class Demo2aStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // S3バケットの作成
    const dataBucket = new s3.Bucket(this, 'DataBucket', {
      bucketName: 'demo-etl-2a-bucket',
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      eventBridgeEnabled: true,
    });

    // 処理済みデータ用のS3バケットの作成
    const processedBucket = new s3.Bucket(this, 'ProcessedBucket', {
      bucketName: 'demo-etl-2a-processed-bucket',
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    // Glueスクリプト用のS3バケットを作成
    const scriptsBucket = new s3.Bucket(this, 'GlueScriptsBucket', {
      bucketName: `demo-etl-2a-scripts-${this.account}-${this.region}`,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    // スクリプトをS3にアップロード
    const notebookScript = new s3deploy.BucketDeployment(this, 'DeployNotebookScript', {
      sources: [s3deploy.Source.asset('scripts')],
      destinationBucket: scriptsBucket,
      destinationKeyPrefix: 'glue-scripts'
    });

    // Glue用のIAMロールの作成
    const glueRole = new iam.Role(this, 'GlueJobRole', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3FullAccess'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonDynamoDBFullAccess'),
      ],
      inlinePolicies: {
        PassRolePolicy: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ['iam:PassRole'],
              resources: ['*'],
            }),
            // Glueセッション関連の権限も追加
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: [
                'glue:CreateSession',
                'glue:GetSession',
                'glue:DeleteSession',
                'glue:RunStatement',
                'glue:GetStatement',
                'glue:CancelStatement'
              ],
              resources: ['*'],
            }),
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ['s3:GetObject', 's3:PutObject', 's3:DeleteObject'],
              resources: ['*'],
            }),
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ['dynamodb:PutItem', 'dynamodb:GetItem', 'dynamodb:DeleteItem'],
              resources: ['*'],
            }),
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: [
                'codewhisperer:GenerateRecommendations',
                'codewhisperer:ListRecommendations'
              ],
              resources: ['*'],
            }),
          ],
        }),
      },
    });

    // DynamoDBテーブルの作成
    const table = new dynamodb.Table(this, 'ProcessedDataTable', {
      tableName: 'demo-etl-2a-table',
      partitionKey: { name: 'id', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // Notebook処理用のGlueジョブ（ジョブA）
    const notebookJob = new glue.CfnJob(this, 'NotebookProcessingJob', {
      name: 'demo-etl-2a-notebook-job',
      role: glueRole.roleArn,
      command: {
        name: 'glueetl',
        pythonVersion: '3',
        scriptLocation: `s3://${scriptsBucket.bucketName}/glue-scripts/demo-etl-2a-notebook.py`,
      },
      defaultArguments: {
        '--job-language': 'python',
        '--continuous-log-logGroup': '/aws-glue/jobs',
        '--input_bucket': dataBucket.bucketName,
      },
      glueVersion: '4.0',
      maxRetries: 2,
      timeout: 60,
      numberOfWorkers: 2,
      workerType: 'G.1X',
    });

    // DynamoDB転送用のGlueジョブ（ジョブB）
    const dynamoDbJob = new glue.CfnJob(this, 'DynamoDBLoadJob', {
      name: 'demo-etl-2a-dynamodb-job',
      role: glueRole.roleArn,
      command: {
        name: 'glueetl',
        pythonVersion: '3',
        scriptLocation: `s3://${scriptsBucket.bucketName}/glue-scripts/process_tsv.py`,
      },
      defaultArguments: {
        '--job-language': 'python',
        '--continuous-log-logGroup': '/aws-glue/jobs',
        '--input_bucket': dataBucket.bucketName,
        '--processed_bucket': processedBucket.bucketName,
        '--dynamodb_table': table.tableName,
      },
      glueVersion: '4.0',
      maxRetries: 2,
      timeout: 60,
      numberOfWorkers: 2,
      workerType: 'G.1X',
    });

    // Glueワークフローの作成
    const workflow = new glue.CfnWorkflow(this, 'GlueWorkflow', {
      name: 'demo-etl-2a-workflow',
      description: 'Workflow for demo ETL process'
    });

    // ジョブAのトリガー（S3イベントで起動）
    const notebookTrigger = new glue.CfnTrigger(this, 'S3EventTrigger', {
      name: 'demo-etl-2a-notebook-trigger',
      type: 'ON_DEMAND',
      actions: [{
        jobName: notebookJob.name,
      }],
      workflowName: workflow.name,
    });

    // ジョブBのトリガー（ジョブAの完了後に起動）
    const dynamoDbTrigger = new glue.CfnTrigger(this, 'NotebookCompletionTrigger', {
      name: 'demo-etl-2a-dynamodb-trigger',
      type: 'CONDITIONAL',
      startOnCreation: true,
      actions: [{
        jobName: dynamoDbJob.name,
      }],
      predicate: {
        conditions: [{
          jobName: notebookJob.name,
          state: 'SUCCEEDED',
          logicalOperator: 'EQUALS'
        }],
        logical: 'AND',
      },
      workflowName: workflow.name,
    });

    // DynamoDB へのアクセス権限を追加
    table.grantWriteData(glueRole);

    // バケットへのアクセス権限を追加
    dataBucket.grantReadWrite(glueRole);
    processedBucket.grantReadWrite(glueRole);

    // スクリプトバケットへのアクセス権限を追加
    scriptsBucket.grantRead(glueRole);

  }
}
