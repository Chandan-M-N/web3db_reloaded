import requests

def fetch_file_from_ipfs_cluster(cid):
    """
    Fetches a file from IPFS Cluster using its CID and saves it locally.

    Parameters:
        cid (str): The Content Identifier (CID) of the file to fetch.
        cluster_api_url (str): The base URL of the IPFS Cluster API (e.g., "http://localhost:9094").
        output_path (str): The path where the fetched file will be saved.

    Returns:
        bool: True if the file is fetched and saved successfully, False otherwise.
    """
    try:
        # Construct the API URL for fetching the file
        url = f"http://localhost:8080/ipfs/{cid}"
        output_path = f"dumps/{cid}.sql"
        
        # Send a GET request to the IPFS Cluster API
        response = requests.get(url, stream=True)
        
        # Check if the request was successful
        if response.status_code == 200:
            # Save the file to the specified output path
            with open(output_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:  # Filter out keep-alive chunks
                        file.write(chunk)
            print(f"File successfully fetched and saved to {output_path}")
            return True
        else:
            print(f"Failed to fetch file: {response.status_code} - {response.text}")
            return False

    except requests.RequestException as e:
        print(f"Error fetching file from IPFS Cluster: {e}")
        return False

fetch_file_from_ipfs_cluster("QmafQeUG4RbGaGwGkWUq8gaVe4PxFTtkUiMuLALEJ7qFxa")