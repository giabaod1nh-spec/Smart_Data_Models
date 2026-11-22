// AuthContext — session-based auth state management
// Calls GET /api/auth/me on load to restore session.
// Listens to custom auth:unauthorized events from httpClient.

import React, { createContext, useCallback, useContext, useEffect, useState } from 'react'
import type { AuthUser } from '@/types/auth'
import { getMe, login as apiLogin, logout as apiLogout } from '@/api/authApi'
import type { LoginRequest } from '@/types/auth'
import { UI_PREVIEW_MODE } from '@/utils/uiPreviewMode'

const PREVIEW_USER: AuthUser = { username: 'preview', role: 'ADMIN' }

interface AuthContextValue {
  user: AuthUser | null
  isLoading: boolean
  isAuthenticated: boolean
  login: (req: LoginRequest) => Promise<void>
  logout: () => Promise<void>
}

const AuthContext = createContext<AuthContextValue | null>(null)

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [user, setUser] = useState<AuthUser | null>(UI_PREVIEW_MODE ? PREVIEW_USER : null)
  const [isLoading, setIsLoading] = useState(!UI_PREVIEW_MODE)

  // Restore session on app load
  useEffect(() => {
    if (UI_PREVIEW_MODE) return
    getMe()
      .then((res) => {
        if (res.data) {
          setUser({ username: res.data.username, role: res.data.role })
        }
      })
      .catch(() => {
        // 401 = not authenticated, expected — stay as null
        setUser(null)
      })
      .finally(() => setIsLoading(false))
  }, [])

  // Listen for 401 from httpClient interceptor
  useEffect(() => {
    if (UI_PREVIEW_MODE) return
    const handleUnauthorized = () => {
      setUser(null)
    }
    window.addEventListener('auth:unauthorized', handleUnauthorized)
    return () => window.removeEventListener('auth:unauthorized', handleUnauthorized)
  }, [])

  const login = useCallback(async (req: LoginRequest) => {
    if (UI_PREVIEW_MODE) {
      setUser(PREVIEW_USER)
      return
    }
    const res = await apiLogin(req)
    if (res.data) {
      setUser({ username: res.data.username, role: res.data.role })
    }
  }, [])

  const logout = useCallback(async () => {
    if (UI_PREVIEW_MODE) {
      setUser(null)
      return
    }
    await apiLogout().catch(() => {/* ignore server error on logout */})
    setUser(null)
  }, [])

  return (
    <AuthContext.Provider
      value={{
        user,
        isLoading,
        isAuthenticated: user !== null,
        login,
        logout,
      }}
    >
      {children}
    </AuthContext.Provider>
  )
}

// eslint-disable-next-line react-refresh/only-export-components
export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext)
  if (!ctx) throw new Error('useAuth must be used within AuthProvider')
  return ctx
}
