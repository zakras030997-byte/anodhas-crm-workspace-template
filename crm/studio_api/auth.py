"""Telegram OIDC authorization code + PKCE; opaque server-side sessions."""
import base64
from datetime import timedelta
from hashlib import sha256
import hmac
import secrets
from urllib.parse import urlencode
import httpx
import jwt
import sqlalchemy as sa
from . import tables as t
from .service import now, Rejected

ISSUER = 'https://oauth.telegram.org'
COOKIE = '__Host-anodhas_session'
LOGIN_COOKIE = '__Host-anodhas_login'

def token_hash(value):
    return sha256(value.encode()).hexdigest()

def challenge(verifier):
    return base64.urlsafe_b64encode(sha256(verifier.encode()).digest()).decode().rstrip('=')

class TelegramIdentity:
    def __init__(self):
        self.keys = jwt.PyJWKClient(ISSUER + '/.well-known/jwks.json', cache_jwk_set=True, lifespan=300, timeout=10)

    def exchange(self, code, verifier, nonce, settings):
        with httpx.Client(timeout=15, follow_redirects=False) as client:
            response = client.post(ISSUER + '/token',
                auth=(settings.oidc_client_id, settings.oidc_client_secret),
                data={'grant_type':'authorization_code','code':code,'redirect_uri':settings.callback,
                      'client_id':settings.oidc_client_id,'code_verifier':verifier})
            response.raise_for_status()
            token = response.json()['id_token']
        if len(token) > 16384:
            raise Rejected('invalid_identity',401)
        key = self.keys.get_signing_key_from_jwt(token)
        return validate_identity(token, key.key, nonce, settings)

def validate_identity(token, key, nonce, settings):
    claims = jwt.decode(token, key, algorithms=['RS256'], audience=settings.oidc_client_id,
        issuer=ISSUER, options={'require':['exp','iat','iss','aud','sub','id','nonce']}, leeway=5)
    if not isinstance(claims['nonce'],str) or not hmac.compare_digest(claims['nonce'],nonce):
        raise Rejected('invalid_identity',401)
    # Telegram profile.id is the numeric account ID. sub is not interchangeable.
    if type(claims['id']) is not int or claims['id'] != settings.owner_id:
        raise Rejected('owner_only',403)
    return claims['id']

class Auth:
    def __init__(self, engine, settings, identity=None):
        self.engine, self.settings = engine, settings
        self.identity = identity or TelegramIdentity()

    def start(self):
        if not self.settings.login_configured:
            raise Rejected('login_not_configured',503)
        state, binding, verifier, nonce = [secrets.token_urlsafe(32) for _ in range(4)]
        with self.engine.begin() as conn:
            # One-owner app: bound unauthenticated attempt storage across workers.
            conn.execute(sa.select(sa.func.pg_advisory_xact_lock(-6681692026090701)))
            conn.execute(t.logins.delete().where(t.logins.c.expires_at < now()))
            if conn.scalar(sa.select(sa.func.count()).select_from(t.logins)) >= 30:
                raise Rejected('login_rate_limited',429)
            conn.execute(t.logins.insert().values(state_hash=token_hash(state),binding_hash=token_hash(binding),verifier=verifier,nonce=nonce,expires_at=now()+timedelta(minutes=5)))
        url = ISSUER + '/auth?' + urlencode(dict(client_id=self.settings.oidc_client_id,
            redirect_uri=self.settings.callback,response_type='code',scope='openid profile',state=state,
            nonce=nonce,code_challenge=challenge(verifier),code_challenge_method='S256'))
        return url, binding

    def callback(self, state, code, binding):
        if not self.settings.login_configured or not state or not code or not binding:
            raise Rejected('invalid_login',401)
        if max(map(len,(state,code,binding))) > 4096:
            raise Rejected('invalid_login',401)
        with self.engine.begin() as conn:
            attempt = conn.execute(sa.select(t.logins).where(t.logins.c.state_hash==token_hash(state)).with_for_update()).mappings().first()
            if not attempt or attempt['expires_at'] <= now() or not hmac.compare_digest(attempt['binding_hash'],token_hash(binding)):
                raise Rejected('invalid_login',401)
            saved = dict(attempt)
            conn.execute(t.logins.delete().where(t.logins.c.state_hash==token_hash(state)))
        # Consume before any network operation. A lost response requires a new login.
        try:
            owner = self.identity.exchange(code,saved['verifier'],saved['nonce'],self.settings)
        except Rejected:
            raise
        except (httpx.HTTPError,jwt.PyJWTError,KeyError,TypeError,ValueError):
            raise Rejected('identity_unavailable',401) from None
        if type(owner) is not int or owner != self.settings.owner_id:
            raise Rejected('owner_only',403)
        token, stamp = secrets.token_urlsafe(48), now()
        with self.engine.begin() as conn:
            conn.execute(t.sessions.delete().where(t.sessions.c.expires_at < stamp))
            conn.execute(t.sessions.insert().values(token_hash=token_hash(token),owner_id=owner,
                created_at=stamp,last_seen_at=stamp,expires_at=stamp+timedelta(hours=12)))
        return token

    def csrf(self, token):
        return hmac.new(self.settings.session_secret.encode(), ('csrf:'+token).encode(),sha256).hexdigest()

    def require(self, token, *, origin=None, csrf=None, mutation=False):
        if not token or len(token)>256:
            raise Rejected('login_required',401)
        with self.engine.begin() as conn:
            row = conn.execute(sa.select(t.sessions).where(t.sessions.c.token_hash==token_hash(token)).with_for_update()).mappings().first()
            stamp = now()
            if not row or row['owner_id']!=self.settings.owner_id or row['expires_at']<=stamp or row['last_seen_at']+timedelta(hours=1)<=stamp:
                raise Rejected('login_required',401)
            if mutation and (origin!=self.settings.origin or not csrf or not hmac.compare_digest(csrf,self.csrf(token))):
                raise Rejected('csrf_rejected',403)
            conn.execute(t.sessions.update().where(t.sessions.c.token_hash==token_hash(token)).values(last_seen_at=stamp))
        return {'owner_id':self.settings.owner_id,'csrf':self.csrf(token),'expires_at':row['expires_at']}

    def logout(self, token):
        with self.engine.begin() as conn:
            conn.execute(t.sessions.delete().where(t.sessions.c.token_hash==token_hash(token)))
