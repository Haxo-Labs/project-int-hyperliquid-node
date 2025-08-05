#!/usr/bin/env python3
"""
ECDSA Signature Utilities

Provides utilities for converting ECDSA signature components (r, s, v) 
to the standard ECDSA concatenated signature format.
"""

def rsv_to_signature(r: str, s: str, v: int) -> str:
    """
    Convert r, s, v components to standard 65-byte concatenated signature format.
    
    Args:
        r: 32-byte r component as hex string (with or without 0x prefix)
        s: 32-byte s component as hex string (with or without 0x prefix)  
        v: Recovery ID as integer (27/28 format)
        
    Returns:
        130-character hex string: 0x{r}{s}{recovery_id}
        
    Example:
        r = "0x689057082784b47a31d68a5a6697227c04fc2eff7b02bf71b792ce9f5d8ead02"
        s = "0x24ac23ad9cabc41d9197f689042f01c152506bf6b943995afd645d6bea52a93a"
        v = 28
        
        Returns: "0x689057082784b47a31d68a5a6697227c04fc2eff7b02bf71b792ce9f5d8ead0224ac23ad9cabc41d9197f689042f01c152506bf6b943995afd645d6bea52a93a01"
    """
    # Remove 0x prefix if present
    r_clean = r[2:] if r.startswith('0x') else r
    s_clean = s[2:] if s.startswith('0x') else s
    
    # Validate input lengths
    if len(r_clean) != 64:
        raise ValueError(f"Invalid r component length: {len(r_clean)} (expected 64 hex chars)")
    if len(s_clean) != 64:
        raise ValueError(f"Invalid s component length: {len(s_clean)} (expected 64 hex chars)")
    if v not in [27, 28]:
        raise ValueError(f"Invalid v value: {v} (expected 27 or 28)")
        
    # Convert v to recovery ID (0 or 1)
    recovery_id = v - 27
    
    # Format as 2-character hex string
    recovery_hex = f"{recovery_id:02x}"
    
    # Concatenate: r + s + recovery_id
    signature = f"0x{r_clean}{s_clean}{recovery_hex}"
    
    return signature

def signature_to_rsv(signature: str) -> tuple[str, str, int]:
    """
    Convert standard 65-byte concatenated signature back to r, s, v components.
    
    Args:
        signature: 130-character hex string (with 0x prefix)
        
    Returns:
        Tuple of (r, s, v) where:
        - r: hex string with 0x prefix
        - s: hex string with 0x prefix  
        - v: integer (27/28 format)
    """
    # Remove 0x prefix
    sig_clean = signature[2:] if signature.startswith('0x') else signature
    
    # Validate length
    if len(sig_clean) != 130:
        raise ValueError(f"Invalid signature length: {len(sig_clean)} (expected 130 hex chars)")
    
    # Extract components
    r = f"0x{sig_clean[:64]}"
    s = f"0x{sig_clean[64:128]}"
    recovery_id = int(sig_clean[128:130], 16)
    
    # Convert recovery ID back to v
    if recovery_id not in [0, 1]:
        raise ValueError(f"Invalid recovery ID: {recovery_id} (expected 0 or 1)")
    
    v = recovery_id + 27
    
    return r, s, v

def validate_signature_format(signature: str) -> bool:
    """
    Validate that a signature string matches the expected format.
    
    Args:
        signature: Signature string to validate
        
    Returns:
        True if valid format, False otherwise
    """
    try:
        if not signature.startswith('0x'):
            return False
        if len(signature) != 132:  # 0x + 130 hex chars
            return False
        # Try to parse as hex
        int(signature[2:], 16)
        return True
    except (ValueError, AttributeError):
        return False


if __name__ == "__main__":
    # Test with sample data
    test_r = "0x689057082784b47a31d68a5a6697227c04fc2eff7b02bf71b792ce9f5d8ead02"
    test_s = "0x24ac23ad9cabc41d9197f689042f01c152506bf6b943995afd645d6bea52a93a"
    test_v = 28
    
    print("Testing signature conversion:")
    print(f"r: {test_r}")
    print(f"s: {test_s}")
    print(f"v: {test_v}")
    
    signature = rsv_to_signature(test_r, test_s, test_v)
    print(f"Standard signature: {signature}")
    
    # Test round-trip conversion
    r_back, s_back, v_back = signature_to_rsv(signature)
    print(f"Round-trip test:")
    print(f"r: {test_r} == {r_back} -> {test_r == r_back}")
    print(f"s: {test_s} == {s_back} -> {test_s == s_back}")
    print(f"v: {test_v} == {v_back} -> {test_v == v_back}") 