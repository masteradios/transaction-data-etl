import os
from dagster import ConfigurableResource, ResourceDependency
from dagster_aws.s3 import S3Resource
from typing import List

class S3BucketManagerResource(ConfigurableResource):
    """
    
    Simplified S3 Resource to interact with S3
    
    """
    s3_base: ResourceDependency[S3Resource]
    bucket_name: str

    def list_files(self, prefix: str = "") -> List[str]:
        """
        Simplified list function.
        
        """
        client = self.s3_base.get_client()
        response = client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
        return [obj["Key"] for obj in response.get("Contents", [])]
    

    def download_object(self,s3Folder,fileName,localFolderPath):
        client= self.s3_base.get_client()
        client.download_file(self.bucket_name,f"{s3Folder}/{fileName}" , f"{localFolderPath}/{fileName}")

    def download_folder(self, s3_prefix: str, local_dir: str):
            client = self.s3_base.get_client()
            
            # 1. List all files in the "folder"
            keys = self.list_files(prefix=s3_prefix)
            
            if not keys:
                print(f"No files found for prefix: {s3_prefix}")
                return

            for key in keys:
                # Skip keys that end in '/' (these are directory placeholders)
                if key.endswith('/'):
                    continue
                
                # 2. Determine local file path
                # This preserves the sub-folder structure locally
                relative_path = os.path.relpath(key, s3_prefix)
                local_file_path = os.path.join(local_dir, relative_path)
                
                # 3. Create local directories if they don't exist
                os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                
                # 4. Download
                print(f"Downloading {key} to {local_file_path}...")
                client.download_file(self.bucket_name, key, local_file_path)

    def read_text(self, key: str) -> str:
        """
        Simplified read function.
        
        """
        client = self.s3_base.get_client()
        response = client.get_object(Bucket=self.bucket_name, Key=key)
        return response["Body"].read().decode("utf-8")

    def write_text(self, key: str, data: str):
        """
        Simplified write function.
        
        """
        client = self.s3_base.get_client()
        client.put_object(Bucket=self.bucket_name, Key=key, Body=data.encode("utf-8"))
    
    def copy_file(self, file: str, source_folder:str ,destination_folder:str, toMove=False,newName =""):
        if newName :
            destination_key = f"{destination_folder}/{newName}"
        else:
            destination_key = f"{destination_folder}/{file}"
        copy_source = {
            'Bucket': self.bucket_name,
            'Key': f"{source_folder}/{file}"
        }
        client = self.s3_base.get_client()
        client.copy(copy_source, self.bucket_name,destination_key)
        if toMove is True:
            client.delete_object(Bucket=self.bucket_name, Key=copy_source['Key'])
