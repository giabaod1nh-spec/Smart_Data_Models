// httpClient.ts — Axios instance with session cookie, 401/403 handling
// Frontend calls Spring Server ONLY. No direct calls to Orion, Projector,
// Kafka, ClickHouse, Python Control API, or SUMO.

import axios, { type AxiosInstance, type AxiosError } from 'axios'

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL as string | undefined

/**
 * Axios instance configured for Spring Server.
 * - withCredentials: true → forwards session cookie automatically
 * - No Bearer token logic (Spring uses HttpSession, not JWT)
 * - No manual cookie handling
 */
export const httpClient: AxiosInstance = axios.create({
  baseURL: API_BASE_URL ?? '',
  withCredentials: true,
  headers: {
    'Content-Type': 'application/json',
    Accept: 'application/json',
  },
  timeout: 15000,
})

/**
 * Response interceptor: centralised 401/403 handling.
 * 401 → dispatch a custom event; AuthProvider listens and redirects to login.
 * 403 → dispatch access-denied event; UI shows Access Denied panel.
 * All other errors are re-thrown for the caller to handle.
 */
httpClient.interceptors.response.use(
  (response) => response,
  (error: AxiosError) => {
    if (error.response?.status === 401) {
      window.dispatchEvent(new CustomEvent('auth:unauthorized'))
    } else if (error.response?.status === 403) {
      window.dispatchEvent(new CustomEvent('auth:forbidden'))
    }
    return Promise.reject(error)
  },
)
