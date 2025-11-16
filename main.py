import os
import json
import mimetypes
import cloudinary
import cloudinary.api
import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError, ConnectionClosedError
from botocore.config import Config
import requests
from pathlib import Path
import time
from dotenv import load_dotenv
load_dotenv()

CLOUDINARY_CLOUD_NAME = os.getenv("CLOUDINARY_CLOUD_NAME")
CLOUDINARY_API_KEY = os.getenv("CLOUDINARY_API_KEY")
CLOUDINARY_API_SECRET = os.getenv("CLOUDINARY_API_SECRET")

LINODE_ACCESS_KEY = os.getenv("LINODE_ACCESS_KEY")
LINODE_SECRET_KEY = os.getenv("LINODE_SECRET_KEY")
LINODE_BUCKET = os.getenv("LINODE_BUCKET")
LINODE_REGION = os.getenv("LINODE_REGION")
LINODE_ENDPOINT = f"https://{LINODE_REGION}.linodeobjects.com"

# Configure Claudinary
cloudinary.config(
    cloud_name=CLOUDINARY_CLOUD_NAME,
    api_key=CLOUDINARY_API_KEY,
    api_secret=CLOUDINARY_API_SECRET
)

# Configure boto3 with retry settings and timeouts
# Note: Linode Object Storage requires checksum calculation
boto_config = Config(
    retries={
        'max_attempts': 3,
        'mode': 'standard'
    },
    connect_timeout=120,
    read_timeout=120,
    max_pool_connections=10,
    request_checksum_calculation='when_required'
)

def get_s3_client():
    """Create a fresh S3 client instance"""
    return boto3.client(
        's3',
        endpoint_url=LINODE_ENDPOINT,
        aws_access_key_id=LINODE_ACCESS_KEY,
        aws_secret_access_key=LINODE_SECRET_KEY,
        region_name=LINODE_REGION,
        config=boto_config
    )

s3_client = get_s3_client()

def verify_linode_connection():
    """Verify Linode connection and bucket access"""
    try:
        print(f"\nVerifying Linode connection...")
        print(f"  Endpoint: {LINODE_ENDPOINT}")
        print(f"  Bucket: {LINODE_BUCKET}")
        print(f"  Region: {LINODE_REGION}")
        
        # Try to head the bucket (check if it exists and we have access)
        client = get_s3_client()
        client.head_bucket(Bucket=LINODE_BUCKET)
        print(f"  ✓ Bucket verified and accessible")
        return True
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        if error_code == '404':
            print(f"  ✗ Error: Bucket '{LINODE_BUCKET}' not found")
        elif error_code == '403':
            print(f"  ✗ Error: Access denied to bucket '{LINODE_BUCKET}'")
        else:
            print(f"  ✗ Error verifying bucket: {error_code} - {e}")
        return False
    except Exception as e:
        print(f"  ✗ Error connecting to Linode: {type(e).__name__}: {e}")
        return False

def make_object_public(s3_key):
    """Make an existing object public by updating its ACL"""
    try:
        client = get_s3_client()
        client.put_object_acl(
            Bucket=LINODE_BUCKET,
            Key=s3_key,
            ACL='public-read'
        )
        return True
    except Exception as e:
        print(f"  Error making object public: {e}")
        return False


def get_all_claudinary_resources(resource_type='image', max_results=1000):
    """Get all resources of a given type from Claudinary."""
    resources = []
    next_cursor = None
    print(f"Fetching all {resource_type} resources from Claudinary...")
     
    while True:
        try:
            if next_cursor:
                result = cloudinary.api.resources(
                    type='upload',
                    resource_type=resource_type,
                    max_results=max_results,
                    next_cursor=next_cursor
                )
                
            else:
                result = cloudinary.api.resources(
                    type='upload',
                    resource_type=resource_type,
                    max_results=max_results
                )
            resources.extend(result['resources'])
            print(f"Fetched {len(resources)} resources so far...")
            
            if 'next_cursor' in result:
                next_cursor = result['next_cursor']
            else:
                break
            
            
        except Exception as e:
            print(f"Error fetching resources: {e}")
            break
    
    return resources

def download_from_claudinary(url,local_path):
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print(f"Downloaded {url} to {local_path}")
        return local_path
        
    except requests.RequestException as e:
        print(f"Error downloading {url}: {e}")
        return None
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        return None
        
        
def upload_to_linode(local_path, s3_key, max_retries=3):
    """Upload file to Linode with retry logic using put_object for better control"""
    # Get file size and content type
    file_size = os.path.getsize(local_path)
    content_type, _ = mimetypes.guess_type(local_path)
    if not content_type:
        # Default to image/jpeg if we can't determine
        content_type = 'image/jpeg'
    
    print(f"  File size: {file_size:,} bytes, Content-Type: {content_type}")
    print(f"  S3 Key: {s3_key}")
    
    # Use put_object for files under 5MB (more reliable for S3-compatible APIs)
    # Use upload_file with multipart for larger files
    use_multipart = file_size > 5 * 1024 * 1024  # 5MB threshold
    
    for attempt in range(1, max_retries + 1):
        # Create a fresh client for each attempt to avoid connection pool issues
        client = get_s3_client()
        
        try:
            if use_multipart:
                print(f"  Using multipart upload for large file...")
                # Use upload_file for large files (handles multipart automatically)
                # upload_file handles file opening internally
                try:
                    client.upload_file(
                        local_path,
                        LINODE_BUCKET,
                        s3_key,
                        ExtraArgs={
                            'ContentType': content_type,
                            'ACL': 'public-read'
                        },
                        Config=TransferConfig(
                            multipart_threshold=5 * 1024 * 1024,  # 5MB
                            multipart_chunksize=5 * 1024 * 1024  # 5MB chunks
                        )
                    )
                except Exception as upload_error:
                    # If upload_file fails, fall back to put_object
                    print(f"  Multipart upload failed, trying direct upload...")
                    print(f"  Error: {type(upload_error).__name__}: {str(upload_error)[:200]}")
                    with open(local_path, 'rb') as file_data:
                        file_content = file_data.read()
                        client.put_object(
                            Bucket=LINODE_BUCKET,
                            Key=s3_key,
                            Body=file_content,
                            ContentType=content_type,
                            ACL='public-read'
                        )
            else:
                # Use put_object for smaller files (more reliable for S3-compatible APIs)
                # Try streaming approach first (more memory efficient)
                try:
                    print(f"  Attempting streaming upload...")
                    with open(local_path, 'rb') as file_data:
                        response = client.put_object(
                            Bucket=LINODE_BUCKET,
                            Key=s3_key,
                            Body=file_data,
                            ContentType=content_type,
                            ACL='public-read'
                        )
                    print(f"  Upload response: ETag={response.get('ETag', 'N/A')}")
                except Exception as stream_error:
                    # If streaming fails, try reading into memory
                    print(f"  Streaming failed: {type(stream_error).__name__}")
                    print(f"  Trying in-memory upload...")
                    with open(local_path, 'rb') as file_data:
                        file_content = file_data.read()
                    
                    print(f"  Uploading {len(file_content):,} bytes...")
                    response = client.put_object(
                        Bucket=LINODE_BUCKET,
                        Key=s3_key,
                        Body=file_content,
                        ContentType=content_type,
                        ACL='public-read'
                    )
                    print(f"  Upload response: ETag={response.get('ETag', 'N/A')}")
            
            print(f"Uploaded {local_path} to s3://{LINODE_BUCKET}/{s3_key}")
            return True
            
        except ConnectionClosedError as e:
            # Catch ConnectionClosedError specifically
            if attempt < max_retries:
                wait_time = 2 ** attempt  # Exponential backoff: 2, 4, 8 seconds
                print(f"  Connection closed error (attempt {attempt}/{max_retries})")
                print(f"  Error details: {str(e)[:300]}")
                print(f"  Retrying in {wait_time}s...")
                time.sleep(wait_time)
                # Force close the client connection
                try:
                    client.close()
                except:
                    pass
            else:
                print(f"Error uploading to Linode after {max_retries} attempts")
                print(f"  Full error: {e}")
                print(f"  This might indicate:")
                print(f"    - Network connectivity issues")
                print(f"    - Incorrect endpoint URL")
                print(f"    - Bucket permissions problem")
                print(f"    - SSL/TLS certificate issues")
                return False
                
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_message = str(e)
            
            # Retry on timeout and transient errors
            retryable_errors = ('RequestTimeout', 'SlowDown', 'ServiceUnavailable', 
                              'InternalError', 'InvalidRequest', 'RequestTimeTooSkewed')
            
            if any(err in error_code or err in error_message for err in retryable_errors) and attempt < max_retries:
                wait_time = 2 ** attempt
                print(f"  {error_code} error (attempt {attempt}/{max_retries}), retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"Error uploading to Linode: {e}")
                print(f"  Error code: {error_code}")
                return False
                
        except ConnectionError as e:
            # Catch other connection-related errors
            if attempt < max_retries:
                wait_time = 2 ** attempt
                print(f"  Connection error (attempt {attempt}/{max_retries}), retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"Error uploading to Linode after {max_retries} attempts: {e}")
                return False
                
        except Exception as e:
            if attempt < max_retries:
                wait_time = 2 ** attempt
                print(f"  Unexpected error (attempt {attempt}/{max_retries}): {type(e).__name__}, retrying in {wait_time}s...")
                print(f"  Error details: {str(e)[:200]}")
                time.sleep(wait_time)
            else:
                print(f"Unexpected error uploading to Linode: {type(e).__name__}: {e}")
                return False
        finally:
            # Clean up client connection
            try:
                client.close()
            except:
                pass
    
    return False
    
def transfer_claudinary_linode(resources, temp_dir, mapping_file='url_mapping.json'):
    """Transfer resources from Claudinary to Linode"""
    # Creating a temporary path
    temp_dir = Path(temp_dir)
    temp_dir.mkdir(parents=True, exist_ok=True)
    successful_count = 0
    failed_count = 0
    
    # Initialize URL mapping dictionary
    url_mapping = {}
    
    # Load existing mapping if file exists
    if os.path.exists(mapping_file):
        try:
            with open(mapping_file, 'r') as f:
                url_mapping = json.load(f)
            print(f"Loaded existing mapping with {len(url_mapping)} entries")
            print(f"  Will skip already uploaded images and resume from remaining ones.")
        except Exception as e:
            print(f"Warning: Could not load existing mapping file: {e}")
            url_mapping = {}
    
    skipped_count = 0
    for idx, resource in enumerate(resources, 1):
        public_id = resource['public_id']
        cloudinary_url = resource['secure_url']
        format_ext = resource.get('format', 'jpg')
        
        # Preserve folder structure
        s3_key = f"{public_id}.{format_ext}"
        local_path = os.path.join(temp_dir, f"{public_id.replace('/', '_')}.{format_ext}")
        
        # Skip if already uploaded (check if Cloudinary URL exists in mapping)
        if cloudinary_url in url_mapping:
            skipped_count += 1
            print(f"\n[{idx}/{len(resources)}] Skipping {public_id} (already uploaded)")
            continue
        
        print(f"\n[{idx}/{len(resources)}] Processing: {public_id}")
        
        # Download from Cloudinary
        print(f"  Downloading from Cloudinary...")
        if not download_from_claudinary(cloudinary_url, local_path):
            failed_count += 1
            continue
        
        # Upload to Linode
        print(f"  Uploading to Linode...")
        if upload_to_linode(local_path, s3_key):
            successful_count += 1
            
            # Build Linode URL
            linode_url = f"{LINODE_ENDPOINT}/{LINODE_BUCKET}/{s3_key}"
            
            # Add to mapping
            url_mapping[cloudinary_url] = linode_url
            
            # Save mapping incrementally after each successful upload
            try:
                with open(mapping_file, 'w') as f:
                    json.dump(url_mapping, f, indent=2)
                print(f"  ✓ Success - Mapping saved")
            except Exception as e:
                print(f"  ✓ Success - Warning: Could not save mapping: {e}")
        else:
            failed_count += 1
            print(f"  ✗ Failed")
        
        # Clean up local file
        try:
            os.remove(local_path)
        except:
            pass
        
        # Rate limiting (optional)
        time.sleep(0.1)
    
    return successful_count, failed_count, skipped_count, url_mapping


def main():
    print("=" * 60)
    print("Cloudinary to Linode Transfer Script")
    print("=" * 60)
    
    # Verify Linode connection first
    if not verify_linode_connection():
        print("\n⚠️  Warning: Could not verify Linode connection.")
        print("   The script will continue, but uploads may fail.")
        response = input("   Continue anyway? (yes/no): ")
        if response.lower() != 'yes':
            print("Transfer cancelled.")
            return
    
    # Fetch all resources
    resources = get_all_claudinary_resources(resource_type='image')
    print(f"\nTotal resources found: {len(resources)}")
    
    if not resources:
        print("No resources found. Exiting.")
        return
    
    # Confirm transfer
    print(f"\n⚠️  This will transfer ALL {len(resources)} images from Cloudinary to Linode.")
    print(f"   The mapping will be saved to url_mapping.json")
    response = input(f"\nProceed with transferring {len(resources)} images? (yes/no): ")
    if response.lower() != 'yes':
        print("Transfer cancelled.")
        return
    
    # Transfer
    print("\nStarting transfer...\n")
    successful, failed, skipped, url_mapping = transfer_claudinary_linode(resources, temp_dir='assets')
    
    # Final save of mapping (redundant but ensures it's saved)
    mapping_file = 'url_mapping.json'
    try:
        with open(mapping_file, 'w') as f:
            json.dump(url_mapping, f, indent=2)
        print(f"\nURL mapping saved to {mapping_file}")
    except Exception as e:
        print(f"\nWarning: Could not save final mapping: {e}")
    
    # Summary
    print("\n" + "=" * 60)
    print("Transfer Complete!")
    print(f"Successful: {successful}")
    print(f"Skipped (already uploaded): {skipped}")
    print(f"Failed: {failed}")
    print(f"Total mappings: {len(url_mapping)}")
    print(f"Mapping file: {mapping_file}")
    print("=" * 60)

if __name__ == "__main__":
    main()