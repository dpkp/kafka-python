import datetime
import hashlib
import hmac
import json
import string

from kafka.vendor.six.moves import urllib


class AwsMskIamClient:
    UNRESERVED_CHARS = string.ascii_letters + string.digits + '-._~'

    def __init__(self, host, access_key, secret_key, region, token=None):
        """
        Arguments:
            host (str): The hostname of the broker.
            access_key (str): An AWS_ACCESS_KEY_ID.
            secret_key (str): An AWS_SECRET_ACCESS_KEY.
            region (str): An AWS_REGION.
            token (Optional[str]): An AWS_SESSION_TOKEN if using temporary
                credentials.
        """
        self.algorithm = 'AWS4-HMAC-SHA256'
        self.expires = '900'
        self.hashfunc = hashlib.sha256
        self.headers = [
            ('host', host)
        ]
        self.version = '2020_10_22'

        self.service = 'kafka-cluster'
        self.action = '{}:Connect'.format(self.service)

        now = datetime.datetime.utcnow()
        self.datestamp = now.strftime('%Y%m%d')
        self.timestamp = now.strftime('%Y%m%dT%H%M%SZ')

        self.host = host
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.token = token

    @property
    def _credential(self):
        return '{0.access_key}/{0._scope}'.format(self)

    @property
    def _scope(self):
        return '{0.datestamp}/{0.region}/{0.service}/aws4_request'.format(self)

    @property
    def _signed_headers(self):
        """
        Returns (str):
            An alphabetically sorted, semicolon-delimited list of lowercase
            request header names.
        """
        return ';'.join(sorted(k.lower() for k, _ in self.headers))

    @property
    def _canonical_headers(self):
        """
        Returns (str):
            A newline-delited list of header names and values.
            Header names are lowercased.
        """
        return '\n'.join(map(':'.join, self.headers)) + '\n'

    @property
    def _canonical_request(self):
        """
        Returns (str):
            An AWS Signature Version 4 canonical request in the format:
                <Method>\n
                <Path>\n
                <CanonicalQueryString>\n
                <CanonicalHeaders>\n
                <SignedHeaders>\n
                <HashedPayload>
        """
        # The hashed_payload is always an empty string for MSK.
        hashed_payload = self.hashfunc(b'').hexdigest()
        return '\n'.join((
            'GET',
            '/',
            self._canonical_querystring,
            self._canonical_headers,
            self._signed_headers,
            hashed_payload,
        ))

    @property
    def _canonical_querystring(self):
        """
        Returns (str):
            A '&'-separated list of URI-encoded key/value pairs.
        """
        params = []
        params.append(('Action', self.action))
        params.append(('X-Amz-Algorithm', self.algorithm))
        params.append(('X-Amz-Credential', self._credential))
        params.append(('X-Amz-Date', self.timestamp))
        params.append(('X-Amz-Expires', self.expires))
        if self.token:
            params.append(('X-Amz-Security-Token', self.token))
        params.append(('X-Amz-SignedHeaders', self._signed_headers))

        return '&'.join(self._uriencode(k) + '=' + self._uriencode(v) for k, v in params)

    @property
    def _signing_key(self):
        """
        Returns (bytes):
            An AWS Signature V4 signing key generated from the secret_key, date,
            region, service, and request type.
        """
        key = self._hmac(('AWS4' + self.secret_key).encode('utf-8'), self.datestamp)
        key = self._hmac(key, self.region)
        key = self._hmac(key, self.service)
        key = self._hmac(key, 'aws4_request')
        return key

    @property
    def _signing_str(self):
        """
        Returns (str):
            A string used to sign the AWS Signature V4 payload in the format:
                <Algorithm>\n
                <Timestamp>\n
                <Scope>\n
                <CanonicalRequestHash>
        """
        canonical_request_hash = self.hashfunc(self._canonical_request.encode('utf-8')).hexdigest()
        return '\n'.join((self.algorithm, self.timestamp, self._scope, canonical_request_hash))

    def _uriencode(self, msg):
        """
        Arguments:
            msg (str): A string to URI-encode.

        Returns (str):
            The URI-encoded version of the provided msg, following the encoding
            rules specified: https://github.com/aws/aws-msk-iam-auth#uriencode
        """
        return urllib.parse.quote(msg, safe=self.UNRESERVED_CHARS)

    def _hmac(self, key, msg):
        """
        Arguments:
            key (bytes): A key to use for the HMAC digest.
            msg (str): A value to include in the HMAC digest.
        Returns (bytes):
            An HMAC digest of the given key and msg.
        """
        return hmac.new(key, msg.encode('utf-8'), digestmod=self.hashfunc).digest()

    def first_message(self):
        """
        Returns (bytes):
            An encoded JSON authentication payload that can be sent to the
            broker.
        """
        signature = hmac.new(
            self._signing_key,
            self._signing_str.encode('utf-8'),
            digestmod=self.hashfunc,
        ).hexdigest()
        msg = {
            'version':  self.version,
            'host': self.host,
            'user-agent': 'kafka-python',
            'action': self.action,
            'x-amz-algorithm': self.algorithm,
            'x-amz-credential': self._credential,
            'x-amz-date': self.timestamp,
            'x-amz-signedheaders': self._signed_headers,
            'x-amz-expires': self.expires,
            'x-amz-signature': signature,
        }
        if self.token:
            msg['x-amz-security-token'] = self.token

        return json.dumps(msg, separators=(',', ':')).encode('utf-8')
