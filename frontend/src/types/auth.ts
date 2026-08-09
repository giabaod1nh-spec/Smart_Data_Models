// auth.ts — Auth types from verified Spring DTOs

/** POST /api/auth/login request body */
export interface LoginRequest {
  username: string
  password: string
}

/** Data field of ApiResponse<LoginResponse> */
export interface LoginResponse {
  username: string
  role: string
}

/** Client-side auth state */
export interface AuthUser {
  username: string
  role: string
}
