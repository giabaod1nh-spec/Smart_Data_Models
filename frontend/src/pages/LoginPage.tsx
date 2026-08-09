// LoginPage.tsx — Login screen matching reference design aesthetic

import React, { useState } from 'react'
import { useNavigate, useLocation } from 'react-router-dom'
import { Eye, EyeOff, TrafficCone, AlertCircle, Loader2 } from 'lucide-react'
import { useAuth } from '@/features/auth/AuthContext'
import type { AxiosError } from 'axios'

interface LocationState {
  from?: { pathname: string }
}

export function LoginPage() {
  const { login, isAuthenticated } = useAuth()
  const navigate = useNavigate()
  const location = useLocation()
  const state = location.state as LocationState | null

  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [showPassword, setShowPassword] = useState(false)
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // If already authenticated, redirect
  React.useEffect(() => {
    if (isAuthenticated) {
      const destination = state?.from?.pathname ?? '/analytics'
      navigate(destination, { replace: true })
    }
  }, [isAuthenticated, navigate, state])

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!username.trim() || !password) return

    setIsLoading(true)
    setError(null)

    try {
      await login({ username: username.trim(), password })
      const destination = state?.from?.pathname ?? '/analytics'
      navigate(destination, { replace: true })
    } catch (err) {
      const axiosErr = err as AxiosError<{ message?: string }>
      const status = axiosErr.response?.status
      if (status === 401) {
        setError('Invalid username or password.')
      } else if (status === 503 || status === 502) {
        setError('Server is temporarily unavailable. Please try again.')
      } else if (!axiosErr.response) {
        setError('Cannot connect to the server. Please check your network.')
      } else {
        setError('Login failed. Please try again.')
      }
    } finally {
      setIsLoading(false)
    }
  }

  return (
    <div
      className="min-h-screen flex flex-col items-center justify-center p-4 relative overflow-hidden"
      style={{ background: 'var(--bg-base)' }}
    >
      {/* Background grid pattern */}
      <div
        className="absolute inset-0 pointer-events-none"
        style={{
          backgroundImage: `
            linear-gradient(rgba(22,140,255,0.04) 1px, transparent 1px),
            linear-gradient(90deg, rgba(22,140,255,0.04) 1px, transparent 1px)
          `,
          backgroundSize: '40px 40px',
        }}
      />

      {/* Glow effect */}
      <div
        className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-96 h-96 rounded-full pointer-events-none"
        style={{
          background: 'radial-gradient(circle, rgba(22,140,255,0.08) 0%, transparent 70%)',
          filter: 'blur(40px)',
        }}
      />

      {/* Card */}
      <div
        className="relative w-full max-w-md rounded-2xl p-8 border"
        style={{
          background: 'var(--card-bg)',
          borderColor: 'var(--border)',
          boxShadow: '0 24px 64px rgba(0,0,0,0.5)',
        }}
      >
        {/* Logo */}
        <div className="flex flex-col items-center mb-8">
          <div
            className="w-16 h-16 rounded-2xl flex items-center justify-center mb-4"
            style={{ background: 'linear-gradient(135deg, #168CFF 0%, #8B5CF6 100%)' }}
          >
            <TrafficCone size={32} className="text-white" />
          </div>
          <h1 className="text-xl font-bold" style={{ color: 'var(--text-primary)' }}>
            Smart Traffic System
          </h1>
          <p className="text-sm mt-1" style={{ color: 'var(--text-secondary)' }}>
            Traffic Intelligence Dashboard
          </p>
        </div>

        {/* Form */}
        <form onSubmit={handleSubmit} noValidate>
          {/* Error message */}
          {error && (
            <div
              className="flex items-center gap-2 rounded-lg px-4 py-3 mb-4 text-sm"
              role="alert"
              aria-live="assertive"
              style={{ background: 'rgba(239,68,68,0.1)', border: '1px solid rgba(239,68,68,0.3)', color: '#EF4444' }}
            >
              <AlertCircle size={16} aria-hidden="true" />
              {error}
            </div>
          )}

          {/* Username */}
          <div className="mb-4">
            <label
              htmlFor="login-username"
              className="block text-sm font-medium mb-1.5"
              style={{ color: 'var(--text-secondary)' }}
            >
              Username
            </label>
            <input
              id="login-username"
              type="text"
              autoComplete="username"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              disabled={isLoading}
              placeholder="Enter username"
              className="w-full rounded-lg px-4 py-2.5 text-sm transition-colors"
              style={{
                background: 'var(--bg-elevated)',
                border: '1px solid var(--border)',
                color: 'var(--text-primary)',
                outline: 'none',
              }}
              onFocus={(e) => (e.target.style.borderColor = 'var(--border-active)')}
              onBlur={(e) => (e.target.style.borderColor = 'var(--border)')}
              aria-describedby={error ? 'login-error' : undefined}
            />
          </div>

          {/* Password */}
          <div className="mb-6">
            <label
              htmlFor="login-password"
              className="block text-sm font-medium mb-1.5"
              style={{ color: 'var(--text-secondary)' }}
            >
              Password
            </label>
            <div className="relative">
              <input
                id="login-password"
                type={showPassword ? 'text' : 'password'}
                autoComplete="current-password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                disabled={isLoading}
                placeholder="Enter password"
                className="w-full rounded-lg px-4 py-2.5 pr-10 text-sm transition-colors"
                style={{
                  background: 'var(--bg-elevated)',
                  border: '1px solid var(--border)',
                  color: 'var(--text-primary)',
                  outline: 'none',
                }}
                onFocus={(e) => (e.target.style.borderColor = 'var(--border-active)')}
                onBlur={(e) => (e.target.style.borderColor = 'var(--border)')}
              />
              <button
                type="button"
                onClick={() => setShowPassword((v) => !v)}
                className="absolute right-3 top-1/2 -translate-y-1/2"
                style={{ color: 'var(--text-muted)', background: 'none', border: 'none', cursor: 'pointer' }}
                aria-label={showPassword ? 'Hide password' : 'Show password'}
              >
                {showPassword ? <EyeOff size={16} /> : <Eye size={16} />}
              </button>
            </div>
          </div>

          {/* Submit */}
          <button
            id="login-submit"
            type="submit"
            disabled={isLoading || !username.trim() || !password}
            className="w-full rounded-lg py-2.5 text-sm font-semibold transition-all flex items-center justify-center gap-2"
            style={{
              background: isLoading || !username.trim() || !password
                ? 'rgba(22,140,255,0.3)'
                : 'var(--blue)',
              color: 'white',
              cursor: isLoading || !username.trim() || !password ? 'not-allowed' : 'pointer',
              border: 'none',
            }}
          >
            {isLoading && <Loader2 size={16} className="animate-spin" aria-hidden="true" />}
            {isLoading ? 'Signing in…' : 'Sign In'}
          </button>
        </form>

        {/* Footer note */}
        <p className="text-center text-xs mt-6" style={{ color: 'var(--text-muted)' }}>
          Administrator access only. Session is managed server-side.
        </p>
      </div>
    </div>
  )
}
