"""
DynamoDB Storage Handler for FallVision
Handles user data storage in DynamoDB
"""

import boto3
import os
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class DynamoDBStorage:
    """Professional DynamoDB storage handler"""
    
    def __init__(self):
        """Initialize DynamoDB connection"""
        # Get credentials from environment variables
        aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        aws_region = os.getenv('AWS_REGION', 'us-east-1')
        
        if aws_access_key and aws_secret_key:
            self.dynamodb = boto3.resource(
                'dynamodb',
                region_name=aws_region,
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key
            )
        else:
            # Use IAM role if running on EC2
            self.dynamodb = boto3.resource('dynamodb', region_name=aws_region)
        
        self.table_name = 'FallVision-Users'
        self.table = self.dynamodb.Table(self.table_name)
        logger.info(f"DynamoDB initialized - Table: {self.table_name}")
    
    def get_user(self, email):
        """
        Get user by email
        
        Args:
            email: User's email address
            
        Returns:
            User dict or None if not found
        """
        try:
            response = self.table.get_item(Key={'email': email})
            if 'Item' in response:
                logger.info(f"User found: {email}")
                return response['Item']
            return None
        except ClientError as e:
            logger.error(f"Error getting user {email}: {e}")
            return None
    
    def save_user(self, user_data):
        """
        Save or update user
        
        Args:
            user_data: Dict with user information (must include 'email')
            
        Returns:
            Boolean success status
        """
        try:
            self.table.put_item(Item=user_data)
            logger.info(f"User saved: {user_data.get('email')}")
            return True
        except ClientError as e:
            logger.error(f"Error saving user: {e}")
            return False
    
    def user_exists(self, email):
        """
        Check if user exists
        
        Args:
            email: User's email address
            
        Returns:
            Boolean
        """
        return self.get_user(email) is not None
    
    def get_all_users(self):
        """
        Get all users (for admin purposes)
        
        Returns:
            List of user dicts
        """
        try:
            response = self.table.scan()
            users = response.get('Items', [])
            logger.info(f"Retrieved {len(users)} users")
            return users
        except ClientError as e:
            logger.error(f"Error getting all users: {e}")
            return []
    
    def delete_user(self, email):
        """
        Delete user by email
        
        Args:
            email: User's email address
            
        Returns:
            Boolean success status
        """
        try:
            self.table.delete_item(Key={'email': email})
            logger.info(f"User deleted: {email}")
            return True
        except ClientError as e:
            logger.error(f"Error deleting user: {e}")
            return False
