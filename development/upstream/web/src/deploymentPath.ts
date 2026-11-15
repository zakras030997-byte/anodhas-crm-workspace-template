// Modified by ANODHAS AI: isolated CRM namespace, preserving internal API paths.
export function developmentPath(path: string): string {
  if (!path.startsWith('/') || path.startsWith('//')) throw new Error('Invalid workspace path');
  return `/development${path}`;
}
export function workspacePath(): string {
  return window.location.pathname.replace(/^\/development(?=\/|$)/, '') || '/';
}
