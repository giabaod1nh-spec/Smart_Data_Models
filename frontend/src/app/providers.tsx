// providers.tsx — Root providers wrapper

import { RouterProvider } from 'react-router-dom'
import { QueryClientProvider } from '@tanstack/react-query'
import { queryClient } from './queryClient'
import { router } from './router'
import { AuthProvider } from '@/features/auth/AuthContext'

/**
 * Root providers in correct nesting order:
 * QueryClientProvider → AuthProvider → RouterProvider
 *
 * AuthProvider is inside QueryClientProvider so auth state can
 * use TanStack Query if needed in the future.
 */
export function Providers() {
  return (
    <QueryClientProvider client={queryClient}>
      <AuthProvider>
        <RouterProvider router={router} />
      </AuthProvider>
    </QueryClientProvider>
  )
}
