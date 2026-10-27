import ipaddress
import socket

from warehouse_delivery.connections import STORED_CONNECTION_FAILURE
from warehouse_delivery.errors import DeliveryError

INTERNAL_HOST_DETAIL = "The host resolves to an internal address."

# RFC 6598 carrier-grade NAT; not caught by is_private.
_SHARED_ADDRESS_SPACE = ipaddress.ip_network("100.64.0.0/10")

_block_internal_hosts = False


def block_internal_hosts(enabled: bool) -> None:
    """Set once at startup from BLOCK_INTERNAL_WAREHOUSE_HOSTS."""
    global _block_internal_hosts
    _block_internal_hosts = enabled


def checked_address(host: str, port: int) -> str | None:
    """The address to connect to in place of the host when internal hosts are
    blocked, so a name the API checked at save time cannot resolve somewhere
    else by the time we connect.

    Raises ``DeliveryError`` if any address the host resolves to is internal,
    by the rules the API applies on SaaS. None when blocking is off or the host
    does not resolve, leaving the driver to report that as usual."""
    if not _block_internal_hosts:
        return None
    try:
        results = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror:
        return None
    addresses = [ipaddress.ip_address(str(r[4][0]).split("%")[0]) for r in results]
    if any(_is_internal(address) for address in addresses):
        raise DeliveryError(STORED_CONNECTION_FAILURE, INTERNAL_HOST_DETAIL)
    return str(addresses[0])


def _is_internal(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
        address = address.ipv4_mapped
    return (
        address.is_loopback
        or address.is_private
        or address.is_link_local
        or address.is_reserved
        or address.is_multicast
        or address.is_unspecified
        or address in _SHARED_ADDRESS_SPACE
    )
