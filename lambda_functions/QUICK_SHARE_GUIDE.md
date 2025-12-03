# Quick Guide: Sharing AWS Access with Friends

## 🚫 DON'T: Share Your Password
Never give your AWS root account email/password to friends!

## ✅ DO: Create IAM Users

### Step-by-Step (5 minutes):

1. **Go to AWS Console**
   - https://console.aws.amazon.com/iam/home#/users

2. **Click "Add users"**

3. **Enter username** (e.g., `friend-name`)

4. **Select "Provide user access to the AWS Management Console"**
   - Choose "I want to create an IAM user"
   - Set password (or let them set it)

5. **Attach Policies:**
   - Search and attach: `ReadOnlyAccess`
   - This gives them view-only access (safe!)

6. **Click "Create user"**

7. **Share these details:**
   ```
   Console URL: https://439464659138.signin.aws.amazon.com/console
   Username: [their-username]
   Password: [temporary-password]
   Region: eu-north-1
   ```

## What They Can Do

✅ View Lambda functions  
✅ Download S3 data  
✅ View logs  
✅ Check schedules  

❌ Cannot delete anything  
❌ Cannot modify functions  
❌ Cannot access billing  

## For AWS CLI Access

If they need to use scripts:

1. Go to IAM → Users → [their-user] → Security credentials
2. Click "Create access key"
3. Choose "CLI"
4. Share the Access Key ID and Secret Access Key securely
5. They run: `aws configure`

## Quick Test

After creating their user, they can test with:

```powershell
# View Lambda functions
aws lambda list-functions --region eu-north-1

# View S3 data
aws s3 ls s3://youngleebigdatabucket/ingest-data/ --recursive

# View logs
aws logs tail /aws/lambda/data-ingest-db-scraper --region eu-north-1
```

## Need Help?

See `SHARE_ACCESS.md` for detailed instructions.

