// AuthContext — session-based auth state management
// Calls GET /api/auth/me on load to restore session.
// Listens to custom auth:unauthorized events from httpClient.

import React, { createContext, useCallback, useContext, useEffect, useState } from 'react'
import type { AuthUser } from '@/types/auth'
import { getMe, login as apiLogin, logout as apiLogout } from '@/api/authApi'
import type { LoginRequest } from '@/types/auth'

interface AuthContextValue {
  user: AuthUser | null
  isLoading: boolean
  isAuthenticated: boolean
  login: (req: LoginRequest) => Promise<void>
  logout: () => Promise<void>
}

const AuthContext = createContext<AuthContextValue | null>(null)

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [user, setUser] = useState<AuthUser | null>(null)
  const [isLoading, setIsLoading] = useState(true)

  // Restore session on app load
  useEffect(() => {
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
    const handleUnauthorized = () => {
      setUser(null)
    }
    window.addEventListener('auth:unauthorized', handleUnauthorized)
    return () => window.removeEventListener('auth:unauthorized', handleUnauthorized)
  }, [])

  const login = useCallback(async (req: LoginRequest) => {
    const res = await apiLogin(req)
    if (res.data) {
      setUser({ username: res.data.username, role: res.data.role })
    }
  }, [])

  const logout = useCallback(async () => {
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
