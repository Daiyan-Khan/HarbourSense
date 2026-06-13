export function getApiBaseUrl() {
  const configured = process.env.REACT_APP_API_BASE_URL || 'http://localhost:8000';
  return configured.replace(/\/$/, '');
}
