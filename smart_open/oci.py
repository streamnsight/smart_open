import logging
import profile

try:
    import oci
    from ocifs import OCIFileSystem
except ImportError:
    MISSING_DEPS = True

import smart_open.utils

logger = logging.getLogger(__name__)

SCHEMES = ("oci")

URI_EXAMPLES = (
    'oci://bucket@tenancy_namespace/bar',
    'oci://bucket@tenancy_namespace/bar/baz',
    )

MISSING_DEPS = False


def parse_uri(uri_as_str):
    """Parse the specified URI into a dict.

    At a bare minimum, the dict must have `schema` member.
    """
    split_uri = smart_open.utils.safe_urlsplit(uri_as_str)
    assert split_uri.scheme in SCHEMES    
    
    return dict(schema=split_uri.scheme)


def open_uri(uri_as_str, mode, transport_params):
    """Return a file-like object pointing to the URI.

    Parameters:

    uri_as_str: str
        The URI to open
    mode: str
        Either "rb" or "wb".  You don't need to implement text modes,
        `smart_open` does that for you, outside of the transport layer.
    transport_params: dict
        Any additional parameters to pass to the `open` function (see below).

    """
    #
    # Parse the URI using parse_uri
    # Consolidate the parsed URI with transport_params, if needed
    # Pass everything to the open function (see below).
    #
    region = transport_params.get('region', None)
    config = {}
    signer = None
    attempts_msg = []
    try:
        # try resource principal
        signer = oci.auth.signers.get_resource_principals_signer()
    except Exception as e:
        signer = None
        attempts_msg.append(str(e))
        pass
    try:
        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
    except Exception as e:
        signer = None
        attempts_msg.append(str(e))
        pass

    try:
        config_file = transport_params.get('config_file', './oci/config')
        profile_name = transport_params.get('profile_name', 'DEFAULT')
        config = oci.config.from_file(file_location=config_file, profile_name=profile_name, region=region)
    except Exception as e:
        attempts_msg.append(str(e))
        raise EnvironmentError('None of the configuration options succeeded: {"\n".join(attempts_msg)}')
    open(uri_as_str, mode, config, signer, region)

    

def open(uri, mode, config, signer, region):
    """This function does the hard work.

    The keyword parameters are the transport_params from the `open_uri`
    function.

    """
    fs = OCIFileSystem(config=config, signer=signer, region=region)
    return fs.open(uri, mode)