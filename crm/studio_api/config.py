from dataclasses import dataclass, field
import os
import re
from urllib.parse import urlsplit


@dataclass(frozen=True)
class OwnerBotSettings:
    """Credential view for the owner transport; never changes client identity."""
    bot_id: int
    bot_username: str
    bot_token: str = field(repr=False)
    owner_id: int
    origin: str
    session_secret: str = field(repr=False)
    bot_login_enabled: bool = True
    pilot_owner_only: bool = True


@dataclass(frozen=True)
class Settings:
    database_url: str = field(repr=False)
    owner_id: int
    origin: str = 'https://example.invalid'
    oidc_client_id: str = ''
    oidc_client_secret: str = field(default='',repr=False)
    session_secret: str = field(default='',repr=False)
    dispatch_secret: str = field(default='',repr=False)
    public_root: str = '/srv/studio-public'
    bot_id: int = 8884933395
    bot_username: str = 'AnodhasStudioBot'
    bot_token: str = field(default='', repr=False)
    bot_login_enabled: bool = False
    webhook_secret: str = field(default='', repr=False)
    crm_bot_id: int = 0
    crm_bot_username: str = ''
    crm_bot_token: str = field(default='', repr=False)
    crm_bot_webhook_secret: str = field(default='', repr=False)
    crm_bot_enabled: bool = False
    media_root: str = '/var/lib/studio/media'
    sheets_credentials_file: str = ''
    leadgen_enabled: bool = False
    sheets_enabled: bool = False
    dispatch_enabled: bool = False
    pilot_owner_only: bool = False
    anod_enabled: bool = False
    anod_welcome_image: str = ''
    anod_contact_image: str = ''
    ai_enabled: bool = False
    ai_api_key: str = field(default='', repr=False)
    ai_model: str = 'gpt-5.6-luna'
    ai_base_url: str = 'https://api.openai.com/v1'
    google_workspace_enabled: bool = False
    google_calendar_enabled: bool = False
    google_calendar_id: str = ''
    owner_email_enabled: bool = False
    owner_email: str = 'contact001@example.invalid'
    gmail_credentials_file: str = ''
    gmail_outreach_enabled: bool = False

    def __post_init__(self):
        parsed = urlsplit(self.origin)
        if parsed.scheme != 'https' or not parsed.netloc or parsed.path or parsed.query or parsed.fragment or parsed.username:
            raise ValueError('A fixed HTTPS origin without path is required')
        if self.owner_id <= 0:
            raise ValueError('A positive owner Telegram ID is required')
        if self.bot_id != 8884933395 or self.bot_username != 'AnodhasStudioBot':
            raise ValueError('Only the separate studio bot is allowed')
        if self.bot_token and self.bot_token.split(':',1)[0] != str(self.bot_id):
            raise ValueError('Bot token identity mismatch')
        if self.crm_bot_enabled:
            if type(self.crm_bot_id) is not int or self.crm_bot_id <= 0 or self.crm_bot_id == self.bot_id:
                raise ValueError('A separate CRM bot identity is required')
            if (not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{4,31}', self.crm_bot_username)
                    or not self.crm_bot_username.lower().endswith('bot')
                    or self.crm_bot_username.lower() == self.bot_username.lower()):
                raise ValueError('A separate CRM bot username is required')
            if (not re.fullmatch(r'[0-9]+:[A-Za-z0-9_-]+', self.crm_bot_token)
                    or self.crm_bot_token.split(':',1)[0] != str(self.crm_bot_id)):
                raise ValueError('CRM bot token identity mismatch')
            if (not re.fullmatch(r'[A-Za-z0-9_-]{32,256}', self.crm_bot_webhook_secret)
                    or self.crm_bot_webhook_secret == self.webhook_secret):
                raise ValueError('A dedicated CRM webhook secret is required')
        if not self.database_url.startswith('postgresql+psycopg://'):
            raise ValueError('PostgreSQL with psycopg is required')
        if len(self.session_secret) < 32:
            raise ValueError('A dedicated session secret of at least 32 characters is required')
        if self.ai_base_url != 'https://api.openai.com/v1' or self.ai_model != 'gpt-5.6-luna':
            raise ValueError('Only the reviewed fixed AI endpoint/model is allowed')
        if self.owner_email != 'contact001@example.invalid':
            raise ValueError('Owner notification destination is fixed')
        for image in (self.anod_welcome_image, self.anod_contact_image):
            if image and image not in (self.origin+'/assets/anod-welcome.png',self.origin+'/assets/anod-contacts.png'):
                raise ValueError('Only reviewed same-origin bot illustrations are allowed')
        if self.google_workspace_enabled and not self.sheets_enabled:
            raise ValueError('Client workspace requires canonical Sheets integration')
        if self.google_calendar_enabled:
            if not self.google_workspace_enabled or not self.sheets_credentials_file:
                raise ValueError('Project calendar requires owner Google workspace credentials')
            if not re.fullmatch(r'[a-z0-9_-]{8,128}@group\.calendar\.google\.com', self.google_calendar_id):
                raise ValueError('A dedicated secondary Google calendar is required')

    @property
    def login_configured(self):
        return bool(self.oidc_client_id and self.oidc_client_secret)

    @property
    def auth_bot(self):
        if not self.crm_bot_enabled:
            return self
        return OwnerBotSettings(self.crm_bot_id,self.crm_bot_username,self.crm_bot_token,
            self.owner_id,self.origin,self.session_secret)

    @property
    def miniapp_url(self):
        return self.origin + '/admin/login/' if self.crm_bot_enabled else ''

    @property
    def callback(self):
        return self.origin + '/api/auth/telegram/callback'

    @classmethod
    def from_env(cls):
        return cls(
            database_url=os.environ.get('STUDIO_DATABASE_URL', ''),
            owner_id=int(os.environ.get('STUDIO_OWNER_ID', '0')),
            origin=os.environ.get('STUDIO_ORIGIN', 'https://example.invalid'),
            oidc_client_id=os.environ.get('STUDIO_OIDC_CLIENT_ID', ''),
            oidc_client_secret=os.environ.get('STUDIO_OIDC_CLIENT_SECRET', ''),
            session_secret=os.environ.get('STUDIO_SESSION_SECRET', ''),
            dispatch_secret=os.environ.get('STUDIO_DISPATCH_SECRET', ''),
            public_root=os.environ.get('STUDIO_PUBLIC_ROOT','/srv/studio-public'),
            bot_token=os.environ.get('STUDIO_BOT_TOKEN',''),
            bot_login_enabled=os.environ.get('STUDIO_BOT_LOGIN_ENABLED') == 'true',
            webhook_secret=os.environ.get('STUDIO_WEBHOOK_SECRET',''),
            crm_bot_id=int(os.environ.get('STUDIO_CRM_BOT_ID','0')),
            crm_bot_username=os.environ.get('STUDIO_CRM_BOT_USERNAME',''),
            crm_bot_token=os.environ.get('STUDIO_CRM_BOT_TOKEN',''),
            crm_bot_webhook_secret=os.environ.get('STUDIO_CRM_BOT_WEBHOOK_SECRET',''),
            crm_bot_enabled=os.environ.get('STUDIO_CRM_BOT_ENABLED') == 'true',
            media_root=os.environ.get('STUDIO_MEDIA_ROOT','/var/lib/studio/media'),
            sheets_credentials_file=os.environ.get('STUDIO_GOOGLE_CREDENTIALS_FILE',''),
            leadgen_enabled=os.environ.get('STUDIO_LEADGEN_ENABLED') == 'true',
            sheets_enabled=os.environ.get('STUDIO_SHEETS_ENABLED') == 'true',
            dispatch_enabled=os.environ.get('STUDIO_DISPATCH_ENABLED') == 'true',
            pilot_owner_only=os.environ.get('STUDIO_PILOT_OWNER_ONLY') == 'true',
            anod_enabled=os.environ.get('STUDIO_ANOD_ENABLED') == 'true',
            anod_welcome_image=os.environ.get('STUDIO_ANOD_WELCOME_IMAGE',''),
            anod_contact_image=os.environ.get('STUDIO_ANOD_CONTACT_IMAGE',''),
            ai_enabled=os.environ.get('STUDIO_AI_ENABLED') == 'true',
            ai_api_key=os.environ.get('STUDIO_AI_API_KEY',''),
            ai_model=os.environ.get('STUDIO_AI_MODEL','gpt-5.6-luna'),
            ai_base_url=os.environ.get('STUDIO_AI_BASE_URL','https://api.openai.com/v1'),
            google_workspace_enabled=os.environ.get('STUDIO_GOOGLE_WORKSPACE_ENABLED') == 'true',
            google_calendar_enabled=os.environ.get('STUDIO_GOOGLE_CALENDAR_ENABLED') == 'true',
            google_calendar_id=os.environ.get('STUDIO_GOOGLE_CALENDAR_ID',''),
            owner_email_enabled=os.environ.get('STUDIO_OWNER_EMAIL_ENABLED') == 'true',
            gmail_credentials_file=os.environ.get('STUDIO_GMAIL_CREDENTIALS_FILE',''),
            gmail_outreach_enabled=os.environ.get('STUDIO_GMAIL_OUTREACH_ENABLED') == 'true',
        )
