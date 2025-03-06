// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

contract AccessControl {
    // Mapping from resource ID to permitted user identifiers
    mapping(string => mapping(string => bool)) public permissions;
    
    // Events
    event PolicyAdded(string resourceId, string subscriberId);
    event PolicyDeleted(string resourceId, string subscriberId);
    event PolicyEvaluated(string resourceId, string requesterId, bool authorized);

    // Function to add policy (share access)
    function addPolicy(
        string memory resourceId,
        string memory subscriberId
    ) public {
        permissions[resourceId][subscriberId] = true;
        emit PolicyAdded(resourceId, subscriberId);
    }

    // Function to delete policy (revoke access)
    function deletePolicy(
        string memory resourceId,
        string memory subscriberId
    ) public {
        permissions[resourceId][subscriberId] = false;
        emit PolicyDeleted(resourceId, subscriberId);
    }
    
    // Function to evaluate policy (check access)
    function evaluatePolicy(
        string memory resourceId,
        string memory requesterId
    ) public view returns (bool) {
        return permissions[resourceId][requesterId];
    }
}