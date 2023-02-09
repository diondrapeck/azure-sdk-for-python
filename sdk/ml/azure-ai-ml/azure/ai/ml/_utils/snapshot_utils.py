import json
import requests
import uuid

from typing import Tuple, Dict

from azure.identity import DefaultAzureCredential
from azure.ai.ml import MLClient

service_url = 'https://{}.api.azureml.ms'
subscription_id = "e9b2ec51-5c94-4fa8-809a-dc1e695e4896"
resource_group = "dipeck-rg1"
workspace_name = "r-bug-bash"

ml_client = MLClient(DefaultAzureCredential(), subscription_id, resource_group, workspace_name)
workspace = ml_client._service_client_10_2022_preview.workspaces.get(
            resource_group_name=resource_group, workspace_name=workspace_name
            )
location = workspace.location
workspace_id = workspace.workspace_id

def get_temp_data_reference(asset_name: str, asset_version: str, request_headers: Dict[str, str], asset_type: str = "models") -> Tuple[str, str]:
    temporary_data_reference_id = str(uuid.uuid4())
    asset_id = f"azureml://locations/{location}/workspaces/{workspace_id}/{asset_type}/{asset_name}/versions/{asset_version}"
    data = {
        "assetId": asset_id,
        "temporaryDataReferenceId": temporary_data_reference_id,
        "temporaryDataReferenceType": "TemporaryBlobReference"
    }
    data_encoded = json.dumps(data).encode('utf-8')   
    s = requests.Session()
    request_url = f"{service_url.format(location)}/assetstore/v1.0/temporaryDataReference/createOrGet"
    response = s.post(request_url, data=data_encoded, headers=request_headers)
    if response.status_code != 200:
        # Not shown here: retry behavior
        print('Unexpected response:', response.status_code, response.txt)
        return
    response_json = json.loads(response.text)
    blob_uri = response_json['blobReferenceForConsumption']['blobUri']
    sas_uri = response_json['blobReferenceForConsumption']['credential']['sasUri']

    return blob_uri, sas_uri

def get_by_hash(hash_str, request_headers):
    # Current hash version "202208"
    from azure.ai.ml._utils._asset_utils import get_content_hash_version
    hash_version = get_content_hash_version()
    # API route is implemented at https://dev.azure.com/msdata/Vienna/_git/vienna?path=/src/azureml-api/src/ProjectContent/Contracts/ISnapshotControllerNewRoutes.cs&version=GBmaster&line=289&lineEnd=290&lineStartColumn=1&lineEndColumn=45&lineStyle=plain&_a=contents
    request_url = "{0}/content/v2.0/subscriptions/{1}/resourceGroups/{2}/providers/Microsoft.MachineLearningServices/workspaces/{3}"\
                  "/snapshots/getByHash?hash={4}&hashVersion={5}"\
        .format(service_url, subscription_id, resource_group, workspace_name,
                hash_str, hash_version)
    s = requests.Session()
    response = s.get(request_url, headers=request_headers)
    if response.status_code == 404:
        # 404 status is the expected response if the snapshot does not exist
        print('Snapshot with hash', hash_str, 'was not found')
        return
    if response.status_code != 200:
        # Not shown here: retry behavior
        print('Unexpected response:', response.status_code, response.txt)
        return
    # Response is SnapshotDto which is defined here: https://dev.azure.com/msdata/Vienna/_git/vienna?path=/src/azureml-api/src/ProjectContent/Contracts/SnapshotDto.cs&version=GBmaster&line=18&lineEnd=18&lineStartColumn=1&lineEndColumn=29&lineStyle=plain&_a=contents
    # The 'name' and 'version' fields are what we need to get the code asset name/version
    response_json = json.loads(response.text)
#    print("this is the full response: ", response_json)
    name = response_json['name']
    version = response_json['version']
    print('Found code asset with name:', name, 'version:', version)


ws_base_url = ml_client._base_url
token = ml_client._credential.get_token(ws_base_url + "/.default").token
request_headers={"Authorization": "Bearer " + token}
request_headers['Content-Type'] = 'application/json; charset=UTF-8'

get_by_hash('953bfd843e9c9f43bbeb4800577ba54f00b89b40169871fd6bf48b83d8404c8f', request_headers)
#get_by_hash('abc-abc-abc', request_headers)

asset_name = "fakeasset"
asset_version = "1"
blob_uri, sas_uri = get_temp_data_reference(asset_name, asset_version, request_headers)
print("blob uri: ", blob_uri)
print("sas uri: ", sas_uri)

