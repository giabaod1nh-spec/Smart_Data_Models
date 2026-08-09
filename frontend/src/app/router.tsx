// router.tsx — Application routes

import { createBrowserRouter } from 'react-router-dom'
import { ProtectedRoute } from '@/features/auth/ProtectedRoute'
import { LoginPage } from '@/pages/LoginPage'
import { AnalyticsOverviewPage } from '@/pages/AnalyticsOverviewPage'
import { IntersectionDetailPage } from '@/pages/IntersectionDetailPage'
import { NotFoundPage } from '@/pages/NotFoundPage'
import { AppShell } from '@/components/layout/AppShell'

export const router = createBrowserRouter([
  {
    path: '/login',
    element: <LoginPage />,
  },
  {
    // Protected shell — all non-login routes live inside AppShell
    element: (
      <ProtectedRoute>
        <AppShell />
      </ProtectedRoute>
    ),
    children: [
      {
        path: '/analytics',
        element: <AnalyticsOverviewPage />,
      },
      {
        path: '/intersections/:intersectionId',
        element: <IntersectionDetailPage />,
      },
      {
        // Default redirect from root
        path: '/',
        element: <AnalyticsOverviewPage />,
      },
    ],
  },
  {
    path: '*',
    element: <NotFoundPage />,
  },
])
