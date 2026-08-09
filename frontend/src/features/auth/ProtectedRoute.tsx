// ProtectedRoute.tsx — Guards routes that require ADMIN session

import React from 'react'
import { Navigate, useLocation } from 'react-router-dom'
import { useAuth } from './AuthContext'

interface Props {
  children: React.ReactNode
}

/**
 * Protects routes that require an ADMIN session.
 * - While auth state is loading: shows a minimal loading screen.
 * - If not authenticated: redirects to /login, preserving the intended path.
 * - If authenticated but not ADMIN: shows Access Denied (403).
 */
export function ProtectedRoute({ children }: Props) {
  const { user, isLoading } = useAuth()
  const location = useLocation()

  if (isLoading) {
    return (
      <div className="flex h-screen items-center justify-center bg-[#06111F]">
        <div className="text-[#A8BDCE] text-sm animate-pulse">Authenticating…</div>
      </div>
    )
  }

  if (!user) {
    // Redirect to login, preserving the route user tried to access
    return <Navigate to="/login" state={{ from: location }} replace />
  }

  if (user.role !== 'ADMIN') {
    return (
      <div className="flex h-screen items-center justify-center bg-[#06111F]">
        <div className="text-center">
          <div className="text-[#EF4444] text-4xl font-bold mb-2">403</div>
          <div className="text-[#F8FAFC] text-lg font-semibold">Access Denied</div>
          <div className="text-[#A8BDCE] text-sm mt-2">
            This page requires Administrator privileges.
          </div>
        </div>
      </div>
    )
  }

  return <>{children}</>
}
