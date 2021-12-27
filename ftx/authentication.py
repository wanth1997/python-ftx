import hmac
import json


def signature(ts: str, method: str, path_url: str, secret: str, post_body=None):
    signature_payload = f"{ts}{method.upper()}/api{path_url}".encode()
    if post_body is not None:
        signature_payload += json.dumps(post_body).encode()
    signature = hmac.new(secret.encode(), signature_payload, "sha256").hexdigest()
    return signature


def ws_signature(ts: str, secret: str):
    signature_payload = f"{ts}websocket_login".encode()
    signature = hmac.new(secret.encode(), signature_payload, "sha256").hexdigest()
    return signature
