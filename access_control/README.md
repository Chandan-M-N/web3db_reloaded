# Access Control Smart Contract

## Deployment Guide (For smart contract)

Skip this step since we already deployed our contract (and we have the contract address) to Ethereum Test Net. Follow the steps below if you're curious to know or want to redeploy the contract for some reason. If we want to update the contract, we need to redeploy it.

### Prerequisites

- Node.js and npm installed
- Hardhat installed in the project
- Metamask wallet with Sepolia ETH
- Infura API key
- Etherscan API key

### Setup Environment

1. Create a `.env` file in the root directory with the following:
   ```
   INFURA_API_KEY=your_infura_api_key
   PRIVATE_KEY=your_wallet_private_key
   ETHERSCAN_API_KEY=your_etherscan_api_key
   ```


### Deployment Steps

1. Go to the root directory:
   ```bash
   cd access_control
   ```

2. Deploy the contract to Sepolia test network:
   ```bash
   npx hardhat run scripts/deploy.js --network sepolia
   ```

3. Verify the contract (wait a few minutes after deployment):
   ```bash
   npx hardhat verify --network sepolia CONTRACT_ADDRESS
   ```
   Replace `CONTRACT_ADDRESS` with the address output from the deployment step.
   

## Interacting with the Contract (Post Deployment)

After deployment, you can interact with the contract using the provided Python script or directly through Etherscan.

## Python Interface

To use the Python interface, install the required packages:

```bash
pip3 install web3 python-dotenv
```
You can add this statement to your requirements.txt or manually execute it using your terminal. 

To test the contract you can run:
```bash
python3 access_control.py
```
You can import access_control.py to your program and call the function add_policy() and evaluate_policy(). This is the same thing as calling the smart contract APIs.

Make sure you have created the .env file with at least the following properties:

   ```
   INFURA_API_KEY=your_infura_api_key
   PRIVATE_KEY=your_wallet_private_key
   ```
