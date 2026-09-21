"""Device fingerprinting and terminal authorization features."""


def is_new_device(device_id: str | None, seen_devices: set[str]) -> bool:
    """Evaluates whether device has not been previously observed for customer or card.

    Args:
        device_id: Unique hardware or browser fingerprint identifier.
        seen_devices: Historical set of verified customer device identifiers.

    Returns:
        True if device_id is non-empty and absent from seen_devices.
    """
    if not device_id:
        return False
    return device_id not in seen_devices


def same_card_two_devices(current_device: str | None, prev_device: str | None) -> bool:
    """Detects consecutive card transactions performed across different devices.

    Args:
        current_device: Device identifier of current transaction.
        prev_device: Device identifier of immediately preceding transaction.

    Returns:
        True if both device identifiers exist and differ.
    """
    if not current_device or not prev_device:
        return False
    return current_device != prev_device
