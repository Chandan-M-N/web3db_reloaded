import requests

def add_file_with_metadata(file_path, cluster_host="localhost", cluster_port=9094):
    """
    Adds a file (including its name and metadata) to the IPFS cluster.

    Parameters:
        file_path (str): The path to the file you want to upload.
        cluster_host (str): The IPFS Cluster host (default is 'localhost').
        cluster_port (int): The IPFS Cluster port (default is 9094).

    Returns:
        dict: A dictionary containing the IPFS hash (CID) or an error message.
    """
    url = f"http://{cluster_host}:{cluster_port}/add"
    
    try:
        # Open the file in binary mode to upload the file itself
        with open(file_path, 'rb') as file:
            # Send POST request to IPFS Cluster
            files = {'file': (file_path.split("/")[-1], file)}
            response = requests.post(url, files=files)

        # Check if the request was successful
        if response.status_code == 200:
            # Return the CID from the response
             print({"cid": response.json().get("cid")})
             return True, response.json().get("cid")
        else:
            print({"error": f"Failed to add file. Status code: {response.status_code}, Response: {response.text}"})
            return False, response.text

    except Exception as e:
        return {"error": str(e)}

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
        output_path = f"dumps/{cid}.json"
        
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