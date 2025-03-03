# Access Control Smart Contract


## Smart Contract Features

- Add access policies for resources
- Evaluate if a user has access to a resource

## Deployment Guide

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

## Interacting with the Contract

After deployment, you can interact with the contract using the provided Python script or directly through Etherscan.

## Python Interface

To use the Python interface, install the required packages:

```bash
pip3 install web3 python-dotenv
```

Then run:
```bash
python3 access_control.py
```