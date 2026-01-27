# Sharing AWS Access with Team Members

## ⚠️ IMPORTANT: Never Share Your AWS Root Account Credentials!

Instead, create IAM (Identity and Access Management) users for each team member.

## Method 1: Create IAM Users (Recommended)

### Step 1: Create IAM User for Each Friend

1. Go to AWS Console → IAM → Users → Add users
2. Enter username (e.g., `john-smith`, `jane-doe`)
3. Select "Provide user access to the AWS Management Console"
4. Choose "I want to create an IAM user"
5. Set a password (or let them set it on first login)
6. Click "Next"

### Step 2: Attach Permissions

Attach these policies:
- **ViewOnlyAccess** (for read-only access)
- **AWSLambdaReadOnlyAccess** (to view Lambda functions)
- **AmazonS3ReadOnlyAccess** (to view/download S3 data)
- **CloudWatchReadOnlyAccess** (to view logs)

**OR** create a custom policy with these permissions:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "lambda:GetFunction",
                "lambda:ListFunctions",
                "lambda:InvokeFunction",
                "s3:GetObject",
                "s3:ListBucket",
                "logs:DescribeLogGroups",
                "logs:GetLogEvents",
                "events:DescribeRule",
                "events:ListRules"
            ],
            "Resource": "*"
        }
    ]
}
```

### Step 3: Share Credentials Securely

Send them:
- **Console URL**: `https://439464659138.signin.aws.amazon.com/console`
- **Account ID**: `439464659138`
- **Username**: Their IAM username
- **Password**: Temporary password (they'll change on first login)

## Method 2: AWS CLI Access Keys (For Scripts/Programs)

If they need to use AWS CLI:

1. Go to IAM → Users → Select user → Security credentials
2. Click "Create access key"
3. Choose "Command Line Interface (CLI)"
4. Download the CSV file with:
   - Access Key ID
   - Secret Access Key
5. Share securely (use password manager, encrypted email, etc.)

They configure it with:
```bash
aws configure
# Enter Access Key ID
# Enter Secret Access Key
# Region: eu-north-1
```

## Method 3: Read-Only Access (Safest)

For friends who only need to view/download data:

**Attach these policies:**
- `ReadOnlyAccess` (AWS managed policy)
- Custom S3 policy for your bucket:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::youngleebigdatabucket",
                "arn:aws:s3:::youngleebigdatabucket/*"
            ]
        }
    ]
}
```

## What They Can Do With Read-Only Access

✅ View Lambda functions  
✅ View EventBridge schedules  
✅ Download S3 data  
✅ View CloudWatch logs  
✅ Check function metrics  

❌ Cannot modify functions  
❌ Cannot delete data  
❌ Cannot change schedules  
❌ Cannot access billing  

## Quick Setup Script for Friends

Create this script for them (`setup_aws_access.ps1`):

```powershell
# AWS CLI Setup Script
Write-Host "=== AWS CLI Setup ===" -ForegroundColor Cyan

# Check if AWS CLI is installed
if (-not (Get-Command aws -ErrorAction SilentlyContinue)) {
    Write-Host "ERROR: AWS CLI not installed!" -ForegroundColor Red
    Write-Host "Download from: https://aws.amazon.com/cli/" -ForegroundColor Yellow
    exit 1
}

Write-Host "Configuring AWS CLI..." -ForegroundColor Yellow
Write-Host "You'll need:"
Write-Host "  - Access Key ID"
Write-Host "  - Secret Access Key"
Write-Host "  - Region: eu-north-1"
Write-Host ""

aws configure

Write-Host ""
Write-Host "Testing connection..." -ForegroundColor Yellow
$test = aws sts get-caller-identity 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Successfully configured!" -ForegroundColor Green
    Write-Host $test
} else {
    Write-Host "❌ Configuration failed!" -ForegroundColor Red
    Write-Host $test
}
```

## Security Best Practices

1. ✅ **Use IAM users** - Never share root account
2. ✅ **Principle of least privilege** - Give only needed permissions
3. ✅ **Rotate access keys** - Change keys every 90 days
4. ✅ **Use MFA** - Enable Multi-Factor Authentication
5. ✅ **Monitor access** - Check CloudTrail logs regularly
6. ✅ **Remove unused users** - Delete users who leave the project

## What to Share

**Safe to share:**
- IAM user credentials (created specifically for them)
- Bucket name: `youngleebigdatabucket`
- Region: `eu-north-1`
- Account ID: `439464659138` (for console login)

**Never share:**
- Your root account email/password
- Your personal access keys
- Your credit card info

## Console Login URL

Your friends can login at:
```
https://439464659138.signin.aws.amazon.com/console
```

Or use the standard AWS Console and select "IAM user" login.

