import os
import json
from web3 import Web3
from dotenv import load_dotenv

class AccessControl:
    def __init__(self, contract_address=None, infura_api_key=None, private_key=None):
        
        load_dotenv()
        
        self.infura_api_key = infura_api_key or os.getenv("INFURA_API_KEY")
        self.private_key = private_key or os.getenv("PRIVATE_KEY")
        
        # Connect to Sepolia network
        self.w3 = Web3(Web3.HTTPProvider(f"https://sepolia.infura.io/v3/{self.infura_api_key}"))
        
        # Check connection
        if not self.w3.is_connected():
            raise Exception("Failed to connect to Ethereum network")
        
        # Set up account
        self.account = self.w3.eth.account.from_key(self.private_key)
        self.address = self.account.address
        print(f"Connected with address: {self.address}")
        
        # Contract information
        self.contract_address = contract_address or "0x0C55E504efA1e13d476d4190ce6B5F8dA59Bb378"
        
        # Contract ABI (generated from Solidity contract)
        self.abi = [
            {
                "anonymous": False,
                "inputs": [
                    {
                        "indexed": False,
                        "internalType": "string",
                        "name": "resourceId",
                        "type": "string"
                    },
                    {
                        "indexed": False,
                        "internalType": "string",
                        "name": "subscriberId",
                        "type": "string"
                    }
                ],
                "name": "PolicyAdded",
                "type": "event"
            },
            {
                "anonymous": False,
                "inputs": [
                    {
                        "indexed": False,
                        "internalType": "string",
                        "name": "resourceId",
                        "type": "string"
                    },
                    {
                        "indexed": False,
                        "internalType": "string",
                        "name": "requesterId",
                        "type": "string"
                    },
                    {
                        "indexed": False,
                        "internalType": "bool",
                        "name": "authorized",
                        "type": "bool"
                    }
                ],
                "name": "PolicyEvaluated",
                "type": "event"
            },
            {
                "inputs": [
                    {
                        "internalType": "string",
                        "name": "resourceId",
                        "type": "string"
                    },
                    {
                        "internalType": "string",
                        "name": "subscriberId",
                        "type": "string"
                    }
                ],
                "name": "addPolicy",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "string",
                        "name": "resourceId",
                        "type": "string"
                    },
                    {
                        "internalType": "string",
                        "name": "requesterId",
                        "type": "string"
                    }
                ],
                "name": "evaluatePolicy",
                "outputs": [
                    {
                        "internalType": "bool",
                        "name": "",
                        "type": "bool"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "string",
                        "name": "",
                        "type": "string"
                    },
                    {
                        "internalType": "string",
                        "name": "",
                        "type": "string"
                    }
                ],
                "name": "permissions",
                "outputs": [
                    {
                        "internalType": "bool",
                        "name": "",
                        "type": "bool"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
            }
        ]
        
        # Create contract instance
        self.contract = self.w3.eth.contract(address=self.contract_address, abi=self.abi)
        
    def add_policy(self, resource_id, subscriber_id):
        # Build transaction
        nonce = self.w3.eth.get_transaction_count(self.address)
        
        tx = self.contract.functions.addPolicy(
            resource_id,
            subscriber_id
        ).build_transaction({
            'from': self.address,
            'gas': 2000000,
            'gasPrice': self.w3.eth.gas_price,
            'nonce': nonce,
        })
        
        # Sign and send transaction
        signed_tx = self.w3.eth.account.sign_transaction(tx, self.private_key)
        tx_hash = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)
        
        # Wait for transaction receipt
        print(f"Transaction sent: {tx_hash.hex()}")
        tx_receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
        print(f"Transaction confirmed in block {tx_receipt['blockNumber']}")
        
        # Process events
        logs = self.contract.events.PolicyAdded().process_receipt(tx_receipt)
        if logs:
            print(f"Policy added: {logs[0]['args']}")
        
        return tx_receipt
    
    def evaluate_policy(self, resource_id, requester_id):
        return self.contract.functions.evaluatePolicy(resource_id, requester_id).call()
    


if __name__ == "__main__":
    # Example usage
    try:
        # Initialize the AccessControl class
        access_control = AccessControl()
        
        # Example: Add a policy
        resource_id = "document123"
        user_id = "alice"
        print(f"Adding policy for {user_id} to access {resource_id}")
        access_control.add_policy(resource_id, user_id)
        
        # Example: Check access
        print(f"Checking if {user_id} can access {resource_id}")
        has_access = access_control.evaluate_policy(resource_id, user_id)
        print(f"Access granted: {has_access}")
        
        # Example: Check access for unauthorized user
        unauthorized_user = "bob"
        print(f"Checking if {unauthorized_user} can access {resource_id}")
        has_access = access_control.evaluate_policy(resource_id, unauthorized_user)
        print(f"Access granted: {has_access}")
        
    except Exception as e:
        print(f"Error: {e}")