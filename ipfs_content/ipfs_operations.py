import requests

def add_file_with_metadata(encrypted_data: bytes, original_filename="data.bin", cluster_host="localhost", cluster_port=9094):
    """
    Adds encrypted data (bytes) to the IPFS cluster while preserving the original filename in metadata.

    Parameters:
        encrypted_data (bytes): The encrypted data to upload.
        original_filename (str): The original filename to preserve in metadata.
        cluster_host (str): The IPFS Cluster host (default is 'localhost').
        cluster_port (int): The IPFS Cluster port (default is 9094).

    Returns:
        tuple: (success: bool, result: Union[str, dict])
               Where result is CID (str) on success or error message (dict)
    """
    url = f"http://{cluster_host}:{cluster_port}/add"
    
    try:
        # Create a file-like object from the encrypted bytes
        files = {'file': (original_filename, encrypted_data)}
        response = requests.post(url, files=files)

        # Check if the request was successful
        if response.status_code == 200:
            print({"cid": response.json().get("cid")})
            return True, response.json().get("cid")
        else:
            print({"error": f"Failed to add file. Status code: {response.status_code}, Response: {response.text}"})
            return False, response.text

    except Exception as e:
        return False, {"error": str(e)}

def fetch_encrypted_data_from_ipfs(cid: str, cluster_api_url: str = "http://localhost:8080") -> bytes:
    """
    Fetches encrypted data from IPFS directly as bytes (no file saved).
    
    Args:
        cid: The Content Identifier (CID) of the encrypted data.
        cluster_api_url: Base URL of the IPFS HTTP API (default: localhost:8080).
    
    Returns:
        bytes: Raw encrypted data (nonce + ciphertext + tag) if successful.
        None: If fetch fails.
    
    Example:
        encrypted_data = fetch_encrypted_data_from_ipfs("QmXoypiz...")
        if encrypted_data:
            decrypted = decrypt_raw_data(encrypted_data, key)  # Your decryption function
    """
    try:
        url = f"{cluster_api_url}/ipfs/{cid}"
        response = requests.get(url, stream=True)
        
        if response.status_code == 200:
            # Collect all chunks into a bytes object
            encrypted_data = b""
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    encrypted_data += chunk
            return encrypted_data
        else:
            print(f"Fetch failed (HTTP {response.status_code}): {response.text}")
            return None

    except requests.RequestException as e:
        print(f"IPFS fetch error: {str(e)}")
        return None