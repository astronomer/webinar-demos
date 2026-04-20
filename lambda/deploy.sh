#!/bin/bash
set -e

if [ -f .env ]; then
    echo "Loading .env file..."
    export $(grep -v '^#' .env | xargs)
else
    echo "Error: .env file not found. Copy .env_example to .env and fill in your values."
    exit 1
fi

if [ -z "$ASTRO_API_URL" ] || [ "$ASTRO_API_URL" = "https://your-org.astronomer.run/your-deployment-id" ]; then
    echo "Error: ASTRO_API_URL not configured in .env"
    exit 1
fi

if [ -z "$ASTRO_API_TOKEN" ] || [ "$ASTRO_API_TOKEN" = "your-astro-api-token" ]; then
    echo "Error: ASTRO_API_TOKEN not configured in .env"
    exit 1
fi

if [ -z "$WEBHOOK_SECRET" ] || [ "$WEBHOOK_SECRET" = "your-webhook-secret" ]; then
    echo "Error: WEBHOOK_SECRET not configured in .env"
    echo "Generate one with: openssl rand -hex 32"
    exit 1
fi

if [ -z "$SLACK_SIGNING_SECRET" ] || [ "$SLACK_SIGNING_SECRET" = "your-slack-signing-secret" ]; then
    echo "Error: SLACK_SIGNING_SECRET not configured in .env"
    echo "Get this from your Slack app's Basic Information page"
    exit 1
fi

if [ -z "$SLACK_BOT_TOKEN" ] || [ "$SLACK_BOT_TOKEN" = "xoxb-your-bot-token" ]; then
    echo "Error: SLACK_BOT_TOKEN not configured in .env"
    echo "Get this from your Slack app's OAuth & Permissions page after installing"
    exit 1
fi

if [ -z "$SLACK_CHANNEL" ] || [ "$SLACK_CHANNEL" = "C0123456789" ]; then
    echo "Error: SLACK_CHANNEL not configured in .env"
    echo "Right-click a channel in Slack -> View channel details -> copy Channel ID"
    exit 1
fi

AWS_REGION=${AWS_REGION:-us-east-1}
STACK_NAME=${STACK_NAME:-hitl-slack-webhook}

echo "Building Lambda..."
sam build --use-container

echo "Deploying to AWS..."

if [ -n "$S3_BUCKET" ]; then
    S3_ARG="--s3-bucket $S3_BUCKET"
else
    S3_ARG="--resolve-s3"
fi

sam deploy \
    --stack-name "$STACK_NAME" \
    --region "$AWS_REGION" \
    $S3_ARG \
    --parameter-overrides \
        "AstroApiUrl=$ASTRO_API_URL" \
        "AstroApiToken=$ASTRO_API_TOKEN" \
        "WebhookSecret=$WEBHOOK_SECRET" \
        "SlackSigningSecret=$SLACK_SIGNING_SECRET" \
        "SlackBotToken=$SLACK_BOT_TOKEN" \
        "SlackChannel=$SLACK_CHANNEL" \
    --capabilities CAPABILITY_IAM \
    --no-confirm-changeset \
    --no-fail-on-empty-changeset

echo ""
echo "Deployment complete!"
echo "Run 'sam list stack-outputs --stack-name $STACK_NAME --region $AWS_REGION' to get the API URL"
