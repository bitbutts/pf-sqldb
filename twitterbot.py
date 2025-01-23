import os
import json
import boto3
import tweepy
import psycopg2
import psycopg2.extras
from botocore.exceptions import ClientError

def get_secret(secret_name):
    """
    Retrieve a secret from AWS Secrets Manager.

    Args:
        secret_name (str): The name of the secret to retrieve.

    Returns:
        dict: The secret as a dictionary.

    Raises:
        Exception: If the secret cannot be retrieved.
    """
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=os.environ.get('AWS_REGION', 'us-east-1')  # Adjust region if necessary
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        print(f"ERROR: Unable to retrieve secret {secret_name}.")
        raise e

    # Secrets Manager returns the secret in either SecretString or SecretBinary
    if 'SecretString' in get_secret_value_response:
        secret = get_secret_value_response['SecretString']
        return json.loads(secret)
    else:
        # If the secret is binary, decode it
        decoded_binary_secret = get_secret_value_response['SecretBinary'].decode('utf-8')
        return json.loads(decoded_binary_secret)

def create_tweet(initiations, tasks_completed, leaderboard_text, pft_sum):
    """
    Construct the tweet content with the provided data.

    Args:
        initiations (int): Number of new initiations.
        tasks_completed (int): Number of tasks completed.
        leaderboard_text (str): Formatted leaderboard text.
        pft_sum (float): Total PFT for completed tasks.

    Returns:
        str: The formatted tweet text.
    """
    tweet = (
        "🚀 Daily PFT Update!\n\n"
        f"✨ {initiations} new initiations yesterday.\n"
        f"✅ {tasks_completed} tasks were completed.\n\n"
        f"🔥 Total PFT for Completed Tasks: {pft_sum}\n\n"
    )
    
    # Twitter character limit is 280. Adjust formatting if necessary.
    if len(tweet) > 280:
        print("WARNING: Tweet exceeds 280 characters. Adjusting content.")
        # Calculate excess length
        excess_length = len(tweet) - 280
        buffer = 3  # for "..."
        truncated_leaderboard_length = len(leaderboard_text) - excess_length - buffer
        if truncated_leaderboard_length > 0:
            truncated_leaderboard = leaderboard_text[:truncated_leaderboard_length] + "..."
            tweet = (
                "🚀 Daily PFT Update!\n\n"
                f"✨ {initiations} new initiations yesterday.\n"
                f"✅ {tasks_completed} tasks were completed.\n\n"
                f"🔥 Total PFT for Completed Tasks: {pft_sum}\n\n"

            )
        # Final check
        if len(tweet) > 280:
            tweet = tweet[:277] + "..."
    
    return tweet

def authenticate_twitter():
    """
    Authenticate with Twitter using Tweepy Client.

    Returns:
        tweepy.Client: Authenticated Tweepy Client object.

    Raises:
        Exception: If authentication fails.
    """
    try:
        # Retrieve Twitter credentials from environment variables
        consumer_key = ''
        consumer_secret = ''
        access_token = ''
        access_token_secret = ''

        if not all([consumer_key, consumer_secret, access_token, access_token_secret]):
            raise ValueError("One or more Twitter credentials are missing in environment variables.")

        # Authenticate as a user with consumer key and secret, and access token and secret
        client = tweepy.Client(
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
            access_token=access_token,
            access_token_secret=access_token_secret,
            wait_on_rate_limit=True
        )
        
        # Test authentication by fetching user info
        response = client.get_me()
        if response.errors:
            print(f"ERROR: Authentication failed - {response.errors}")
            raise Exception("Authentication failed")
        print("Authentication with Twitter successful")
        return client
    except tweepy.TweepyException as e:
        print(f"ERROR: Twitter authentication failed. {e}")
        raise e
    except ValueError as ve:
        print(f"ERROR: {ve}")
        raise ve

def post_tweet(client, tweet_content):
    """
    Post a tweet using the authenticated Tweepy Client.

    Args:
        client (tweepy.Client): Authenticated Tweepy Client object.
        tweet_content (str): The content of the tweet to post.

    Raises:
        Exception: If posting the tweet fails.
    """
    try:
        response = client.create_tweet(text=tweet_content, user_auth=True)
        if response.errors:
            print(f"ERROR: Could not post tweet - {response.errors}")
            raise Exception("Tweet posting failed")
        print("Tweet posted successfully!")
    except tweepy.errors.Forbidden as e:
        print(f"ERROR: Forbidden - {e}")
        raise e
    except tweepy.TweepyException as e:
        print(f"ERROR: Could not post tweet. {e}")
        raise e

def lambda_handler(event, context):
    """
    AWS Lambda handler function.

    Args:
        event (dict): Event data.
        context (object): Lambda context.

    Returns:
        dict: Response object.
    """
    # Define the name of your database secret
    NEON_SECRET_NAME = 'neon_db_pf'  # Replace with your actual Neon DB secret name

    # Retrieve only the database secret from AWS Secrets Manager
    try:
        neon_secrets = get_secret(NEON_SECRET_NAME)
    except Exception as e:
        print("ERROR: Failed to retrieve database secret.")
        print(e)
        return {
            'statusCode': 500,
            'body': "Secrets retrieval error"
        }

    # Extract the connection string for Neon
    connection_string = neon_secrets.get('DB_CONN_STRING')
    if not connection_string:
        print("ERROR: 'DB_CONN_STRING' not found in Neon secrets.")
        return {
            'statusCode': 500,
            'body': "Invalid database secrets"
        }

    # Connect to the Neon database using the connection string
    try:
        conn = psycopg2.connect(connection_string)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    except Exception as e:
        print("ERROR: Could not connect to the Neon database.")
        print(e)
        return {
            'statusCode': 500,
            'body': "Database connection error"
        }

    # Define your queries
    query1 = """
    SELECT 
        COUNT(*) AS tasks_completed,
        ROUND(SUM(amount)) AS pft
    FROM pft_transactions
    WHERE (memo LIKE 'REWARD RESPONSE%%' OR memo LIKE 'Corbanu Reward%%')
      AND (transaction_timestamp AT TIME ZONE 'America/New_York')::date = 
          ((CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date - INTERVAL '1 day')::date 
      AND from_address = 'r4yc85M1hwsegVGZ1pawpZPwj65SVs8PzD'
    """

    query2 = """
    SELECT
        to_address AS user,
        COUNT(*) AS tasks_completed,
        ROUND(SUM(amount)) AS pft
    FROM pft_transactions
    WHERE (memo LIKE 'REWARD RESPONSE%%' OR memo LIKE 'Corbanu Reward%%')
      AND (transaction_timestamp AT TIME ZONE 'America/New_York')::date = 
          ((CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date - INTERVAL '1 day')::date 
      AND from_address = 'r4yc85M1hwsegVGZ1pawpZPwj65SVs8PzD'
    GROUP BY 1
    ORDER BY pft DESC
    LIMIT 5
    """

    query3 = """
    SELECT 
        COUNT(*) AS initiations
    FROM pft_transactions
    WHERE memo NOT LIKE 'REWARD RESPONSE%%'
      AND memo NOT LIKE 'REQUEST_POST_FIAT%%'
      AND memo NOT LIKE 'PROPOSED PF%%'
      AND memo NOT LIKE 'VERIFICATION PROMPT%%'
      AND memo NOT LIKE 'Corbanu Reward%%'
      AND memo NOT LIKE 'Initial PFT Grant Post Initiation%%' 
      AND (transaction_timestamp AT TIME ZONE 'America/New_York')::date = 
          ((CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date - INTERVAL '1 day')::date  
      AND from_address = 'r4yc85M1hwsegVGZ1pawpZPwj65SVs8PzD'
    """

    try:
        # Execute Query 1
        cursor.execute(query1)
        row_q1 = cursor.fetchone()
        tasks_completed = row_q1["tasks_completed"] if row_q1["tasks_completed"] else 0
        pft_sum = row_q1["pft"] if row_q1["pft"] else 0

        # Execute Query 2
        cursor.execute(query2)
        top_rows = cursor.fetchall()

        # Execute Query 3
        cursor.execute(query3)
        row_q3 = cursor.fetchone()
        initiations = row_q3["initiations"] if row_q3["initiations"] else 0

    except Exception as e:
        print("ERROR: Query execution failed.")
        print(e)
        cursor.close()
        conn.close()
        return {
            'statusCode': 500,
            'body': "Query execution error"
        }

    cursor.close()
    conn.close()

    # Construct the leaderboard
    leaderboard_lines = []
    rank = 1
    for row in top_rows:
        to_address = row["user"]
        tasks = row["tasks_completed"]
        pft_for_address = row["pft"]
        # Optionally, format to_address as a Twitter handle if possible
        # Ensure that to_address is a valid Twitter handle or map addresses to handles
        # If to_address is not a Twitter handle, remove '@' or map accordingly
        leaderboard_lines.append(f"{rank}) @{to_address} | tasks: {tasks}, pft: {pft_for_address}")
        rank += 1

    leaderboard_text = "\n".join(leaderboard_lines)

    # Create the tweet content
    tweet_text = create_tweet(initiations, tasks_completed, leaderboard_text, pft_sum)

    # Authenticate with Twitter
    try:
        twitter_client = authenticate_twitter()
    except Exception as e:
        return {
            'statusCode': 500,
            'body': "Twitter authentication error"
        }

    # Check tweet length (optional, as create_tweet already handles it)


    # Post the tweet
    try:
        post_tweet(twitter_client, tweet_text)
    except tweepy.errors.Forbidden as e:
        # Specific handling for 403 Forbidden
        print("ERROR: Forbidden - You might not have the necessary permissions to post tweets.")
        return {
            'statusCode': 403,
            'body': "Forbidden: Insufficient permissions to post tweets."
        }
    except tweepy.TweepyException as e:
        # Handle other Tweepy errors
        print(f"ERROR: An error occurred while posting the tweet. {e}")
        return {
            'statusCode': 500,
            'body': "Error posting tweet"
        }

    return {
        'statusCode': 200,
        'body': "Tweet posted successfully"
    }
