const { expect } = require("chai");
const { ethers } = require("hardhat");

describe("AccessControl", function () {
  let accessControl;
  let owner;
  let addr1;
  let addr2;

  // Test data
  const resourceId1 = "resource1";
  const resourceId2 = "resource2";
  const subscriberId1 = "subscriber1";
  const subscriberId2 = "subscriber2";

  beforeEach(async function () {
    // Get signers
    [owner, addr1, addr2] = await ethers.getSigners();

    // Deploy the AccessControl contract
    const AccessControl = await ethers.getContractFactory("AccessControl");
    accessControl = await AccessControl.deploy();
    
    // No need to call deployed() - this is the change!
  });

  describe("Policy Management", function () {
    it("Should add a policy correctly", async function () {
      await accessControl.addPolicy(resourceId1, subscriberId1);
      expect(await accessControl.permissions(resourceId1, subscriberId1)).to.equal(true);
    });

    it("Should delete a policy correctly", async function () {
      // First add the policy
      await accessControl.addPolicy(resourceId1, subscriberId1);
      expect(await accessControl.permissions(resourceId1, subscriberId1)).to.equal(true);
      
      // Then delete the policy
      await accessControl.deletePolicy(resourceId1, subscriberId1);
      expect(await accessControl.permissions(resourceId1, subscriberId1)).to.equal(false);
    });

    it("Should evaluate a policy correctly", async function () {
      // No permission initially
      expect(await accessControl.evaluatePolicy(resourceId1, subscriberId1)).to.equal(false);
      
      // Add permission
      await accessControl.addPolicy(resourceId1, subscriberId1);
      expect(await accessControl.evaluatePolicy(resourceId1, subscriberId1)).to.equal(true);
      
      // Delete permission
      await accessControl.deletePolicy(resourceId1, subscriberId1);
      expect(await accessControl.evaluatePolicy(resourceId1, subscriberId1)).to.equal(false);
    });

    it("Should handle multiple resources and subscribers correctly", async function () {
      await accessControl.addPolicy(resourceId1, subscriberId1);
      await accessControl.addPolicy(resourceId2, subscriberId2);
      
      expect(await accessControl.evaluatePolicy(resourceId1, subscriberId1)).to.equal(true);
      expect(await accessControl.evaluatePolicy(resourceId2, subscriberId2)).to.equal(true);
      expect(await accessControl.evaluatePolicy(resourceId1, subscriberId2)).to.equal(false);
      expect(await accessControl.evaluatePolicy(resourceId2, subscriberId1)).to.equal(false);
    });
  });

  describe("Events", function () {
    it("Should emit PolicyAdded event when adding a policy", async function () {
      await expect(accessControl.addPolicy(resourceId1, subscriberId1))
        .to.emit(accessControl, "PolicyAdded")
        .withArgs(resourceId1, subscriberId1);
    });

    it("Should emit PolicyDeleted event when deleting a policy", async function () {
      // First add the policy
      await accessControl.addPolicy(resourceId1, subscriberId1);
      
      // Then delete and check for the event
      await expect(accessControl.deletePolicy(resourceId1, subscriberId1))
        .to.emit(accessControl, "PolicyDeleted")
        .withArgs(resourceId1, subscriberId1);
    });

    it("Should allow deleting policies that don't exist", async function () {
      // Deleting a policy that doesn't exist should still work (and set to false)
      await accessControl.deletePolicy(resourceId1, subscriberId1);
      expect(await accessControl.permissions(resourceId1, subscriberId1)).to.equal(false);
    });
  });

  describe("Integration scenarios", function () {
    it("Should handle grant, check, revoke, check cycle", async function () {
      // Initial state - no access
      expect(await accessControl.evaluatePolicy(resourceId1, subscriberId1)).to.equal(false);
      
      // Grant access
      await accessControl.addPolicy(resourceId1, subscriberId1);
      expect(await accessControl.evaluatePolicy(resourceId1, subscriberId1)).to.equal(true);
      
      // Revoke access
      await accessControl.deletePolicy(resourceId1, subscriberId1);
      expect(await accessControl.evaluatePolicy(resourceId1, subscriberId1)).to.equal(false);
      
      // Grant access again
      await accessControl.addPolicy(resourceId1, subscriberId1);
      expect(await accessControl.evaluatePolicy(resourceId1, subscriberId1)).to.equal(true);
    });
  });
});