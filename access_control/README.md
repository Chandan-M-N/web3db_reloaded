# Access Control Smart Contract
   
## Interacting with the Contract (Post Deployment)
Our Access Control Smart Contract is deployed to the Sepolia Ethereum test network.
To interact with the contract, use the provided Python script or directly use Etherscan for fun.

### Etherscan API
API Link: https://sepolia.etherscan.io/address/0xA7a92f579Aa5771B4F77EfB7987266B07Cb2c5Ef#readContract

Click the link, connect your meta mask wallet, and interact with the contract!

### Python Interface

To use the Python interface, make sure you have created the .env file (in the access_control directory) with at least the following properties:

   ```
   INFURA_API_KEY=eb1d43f1429e49fba50e18fbf5ebd4ab
   CONTRACT_ADDRESS=0xA7a92f579Aa5771B4F77EfB7987266B07Cb2c5Ef
   PRIVATE_KEY=your_wallet_private_key
   ```

Install the required packages:

```bash
pip3 install web3 python-dotenv
```
You can add this statement to your requirements.txt or manually execute it using your terminal. 

Import access_control.py to your program and call the function add_policy(), deletePolicy() and evaluate_policy(). This is the same thing as calling the smart contract APIs.
