import { getApiBaseUrl } from './apiConfig';

describe('getApiBaseUrl', () => {
  const original = process.env.REACT_APP_API_BASE_URL;

  afterEach(() => {
    if (original === undefined) {
      delete process.env.REACT_APP_API_BASE_URL;
    } else {
      process.env.REACT_APP_API_BASE_URL = original;
    }
  });

  test('defaults to local backend', () => {
    delete process.env.REACT_APP_API_BASE_URL;
    expect(getApiBaseUrl()).toBe('http://localhost:8000');
  });

  test('strips trailing slash from configured URL', () => {
    process.env.REACT_APP_API_BASE_URL = 'http://api.example:9000/';
    expect(getApiBaseUrl()).toBe('http://api.example:9000');
  });
});
