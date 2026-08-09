// authApi.ts — Auth API calls via Spring Server
// Routes: POST /api/auth/login, GET /api/auth/me, POST /api/auth/logout

import type { ApiResponse } from '@/types/common'
import type { LoginRequest, LoginResponse } from '@/types/auth'
import { httpClient } from './httpClient'

/**
 * POST /api/auth/login
 * Returns ApiResponse<LoginResponse> on success.
 * Throws AxiosError on 401 (invalid credentials) or 5xx (server error).
 */
export async function login(body: LoginRequest): Promise<ApiResponse<LoginResponse>> {
  const res = await httpClient.post<ApiResponse<LoginResponse>>('/api/auth/login', body)
  return res.data
}

/**
 * GET /api/auth/me
 * Returns the current session user. Returns 401 if not authenticated.
 */
export async function getMe(): Promise<ApiResponse<LoginResponse>> {
  const res = await httpClient.get<ApiResponse<LoginResponse>>('/api/auth/me')
  return res.data
}

/**
 * POST /api/auth/logout
 * Clears the server-side session. Returns 200 on success.
 */
export async function logout(): Promise<ApiResponse<null>> {
  const res = await httpClient.post<ApiResponse<null>>('/api/auth/logout')
  return res.data
}
