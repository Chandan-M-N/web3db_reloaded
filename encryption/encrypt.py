from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
import os
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import json

def generate_symmetric_key(password: bytes = None, salt: bytes = None):
    """
    Generates a 256-bit (32-byte) symmetric key using either:
    - Random generation (secure) if no password provided
    - Password-based derivation if password provided
    
    Args:
        password (bytes): Optional password for key derivation
        salt (bytes): Optional salt for key derivation (random if None)
    
    Returns:
        tuple: (key, salt) where key is 32 bytes and salt is 16 bytes
    """
    if password is None:
        # Generate completely random key (most secure)
        return os.urandom(32), None
    
    # Derive key from password (less secure but memorable)
    if salt is None:
        salt = os.urandom(16)  # New salt
        
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=390000,
    )
    key = kdf.derive(password)
    return key, salt


def encrypt_json_string(json_str: str, key: bytes) -> bytes:
    """
    Encrypts a raw JSON string using AES-GCM.
    Input: 
      - json_str: Raw JSON string (e.g., '{"name": "Bob", "age": 25}')
      - key: 32-byte AES key
    Returns: 
      - Combined nonce (12B) + ciphertext + tag (16B) as bytes
    """
    if len(key) != 32:
        raise ValueError("Key must be 32 bytes for AES-256")

    # Convert JSON string to bytes (UTF-8)
    
    json_bytes = json.dumps(json_str).encode('utf-8')

    # Encrypt
    nonce = os.urandom(12)
    cipher = Cipher(algorithms.AES(key), modes.GCM(nonce), backend=default_backend())
    encryptor = cipher.encryptor()
    ciphertext = encryptor.update(json_bytes) + encryptor.finalize()

    return nonce + ciphertext + encryptor.tag


def decrypt_to_json_string(encrypted_data: bytes, key: bytes) -> str:
    """
    Decrypts data to the original JSON string.
    Raises:
      - InvalidTag: If tampering detected
      - UnicodeDecodeError: If decrypted data isn't valid UTF-8
    """
    nonce = encrypted_data[:12]
    ciphertext = encrypted_data[12:-16]
    tag = encrypted_data[-16:]

    cipher = Cipher(algorithms.AES(key), modes.GCM(nonce, tag), backend=default_backend())
    decryptor = cipher.decryptor()
    decrypted_bytes = decryptor.update(ciphertext) + decryptor.finalize()

    return decrypted_bytes.decode('utf-8')  # Convert back to JSON string


