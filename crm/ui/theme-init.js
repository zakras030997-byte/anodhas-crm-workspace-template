(function () {
  let mode = 'light';
  try {
    const stored = localStorage.getItem('anodhas-theme');
    if (['light', 'dark', 'system'].includes(stored)) mode = stored;
  } catch {}
  const dark =
    mode === 'dark' ||
    (mode === 'system' &&
      window.matchMedia &&
      window.matchMedia('(prefers-color-scheme: dark)').matches);
  document.documentElement.dataset.mode = mode;
  document.documentElement.dataset.theme = dark ? 'dark' : 'light';
  document.documentElement.classList.toggle('dark', dark);
})();
